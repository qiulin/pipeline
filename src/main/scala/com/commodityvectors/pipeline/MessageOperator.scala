package com.commodityvectors.pipeline

import scala.collection.mutable
import scala.concurrent.Future

import akka.Done
import org.joda.time.DateTime
import com.typesafe.scalalogging.LazyLogging

import com.commodityvectors.pipeline.MessageComponentType.MessageComponentType
import com.commodityvectors.pipeline.protocol._
import com.commodityvectors.pipeline.state.MessageComponentStateManager
import com.commodityvectors.pipeline.stream.operator.{
  Operator,
  OperatorInput,
  OperatorOutput
}

private class MessageOperator[In, Out](op: DataOperator[In, Out], id: String)(
    stateManager: MessageComponentStateManager)
    extends MessageComponent(op, id)(stateManager)
    with Operator[Message[In], Message[Out]]
    with Snapshottable
    with LazyLogging {

  import MessageOperator._

  // fast context, but need to be careful with it
  import util.ExecutionContexts.Implicits.sameThreadExecutionContext

  override type Snapshot = MessageOperator.State[In, Out]

  override def componentType: MessageComponentType =
    MessageComponentType.Operator

  override val inputs =
    Vector.tabulate(op.inputs.length)(i => new OperatorInput[Message[In]](i))
  override val outputs =
    Vector.tabulate(op.outputs.length)(i => new OperatorOutput[Message[Out]](i))

  private val alignedQueues =
    Vector.tabulate(inputs.length)(_ => mutable.Queue[In]())
  private var snapshotAlign: Option[SnapshotAlign] = None
  private var restoreAlign: Option[SnapshotAlign] = None

  override def run(): Future[Done] = {
    (snapshotAlign, restoreAlign) match {
      case (Some(a), _) =>
        runSnapshotAlign(a)
      case (_, Some(a)) =>
        runRestoreAlign(a)
      case _ =>
        runNormal()
    }
  }

  private def runNormal(): Future[Done] = {
    // process inputs
    val inputsProcessed: Future[Done] = forEachAsync(inputs.indices) { i =>
      // process next input once previous input has been processed
      if (inputs(i).isClosed) {
        op.inputs(i).close()
      }

      // push all messages cached in aligned queue first
      while (alignedQueues(i).nonEmpty) {
        op.inputs(i).push(alignedQueues(i).dequeue())
      }

      if (inputs(i).nonEmpty && op.inputs(i).isEmpty) {
        normalProcess(inputs(i).pull(), i)
      } else {
        Future.successful(Done)
      }
    }

    inputsProcessed.flatMap { _ =>
      // let's see if we received snapshot messages perfectly at the same time
      checkAlign().flatMap { _ =>
        // run operator
        op.run().map { _ =>
          // process outputs
          for (i <- outputs.indices) {
            if (op.outputs(i).nonEmpty) {
              outputs(i).push(Message.user(op.outputs(i).pull()))
            }
          }
          Done
        }
      }
    }
  }

  private def runSnapshotAlign(align: SnapshotAlign): Future[Done] = {
    forEachAsync(inputs.indices) { i =>
      val isEmpty = inputs(i).isEmpty
      val isAligned = align.aligned(i)
      if (!isEmpty && !isAligned) {
        val msg = inputs(i).pull()
        alignToSnapshot(msg, i, align)
      } else {
        Future.successful(Done)
      }
    }
  }

  private def runRestoreAlign(align: SnapshotAlign): Future[Done] = {
    forEachAsync(inputs.indices) { i =>
      val isEmpty = inputs(i).isEmpty
      val isAligned = align.aligned(i)
      if (!isEmpty && !isAligned) {
        val msg = inputs(i).pull()
        alignToRestore(msg, i, align)
      } else {
        Future.successful(Done)
      }
    }
  }

  private def checkAlign(): Future[Done] = {
    (snapshotAlign, restoreAlign) match {
      case (Some(align), _) if align.isComplete =>
        onSystemMessage(align.originalMessage).map { _ =>
          broadcast(align.originalMessage)
          snapshotAlign = None
          Done
        }
      case (_, Some(align)) if align.isComplete =>
        onSystemMessage(align.originalMessage).map { _ =>
          broadcast(align.originalMessage)
          restoreAlign = None
          Done
        }
      case _ =>
        Future.successful(Done)
    }
  }

  private def alignToSnapshot(msg: Message[In],
                              inputIndex: Int,
                              align: SnapshotAlign): Future[Done] = {
    msg match {
      case msg @ Message.user(_, data) =>
        sync {
          alignedQueues(inputIndex).enqueue(data)
          Done
        }
      case msg @ Message.system(_, CreateSnapshot(align.snapshotId, _, _)) =>
        align.aligned(inputIndex) = true
        checkAlign()
      case msg @ Message.system(_, _) =>
        Future.successful(Done)
    }
  }

  private def alignToRestore(msg: Message[In],
                             inputIndex: Int,
                             align: SnapshotAlign): Future[Done] = {
    logger.debug(s"Aligning message($msg) to restore(${align.snapshotId})")
    msg match {
      case msg @ Message.user(_, data) =>
        sync {
          alignedQueues(inputIndex).enqueue(data)
          Done
        }
      case msg @ Message.system(_, RestoreSnapshot(align.snapshotId)) =>
        align.aligned(inputIndex) = true
        checkAlign()
      case msg @ Message.system(_, _) =>
        Future.successful(Done)
    }
  }

  private def normalProcess(msg: Message[In], inputIndex: Int): Future[Done] = {
    msg match {
      case msg @ Message.user(_, data) =>
        sync {
          op.inputs(inputIndex).push(data)
          Done
        }
      case msg @ Message.system(_, CreateSnapshot(snapshotId, _, _)) =>
        sync {
          // start aligning to a snapshot message
          markSnapshotAligned(inputIndex, snapshotId, msg)
          Done
        }
      case msg @ Message.system(_, RestoreSnapshot(snapshotId)) =>
        sync {
          markRestoreAligned(inputIndex, snapshotId, msg)
          Done
        }
      case msg @ Message.system(_, _) =>
        // broadcast other system messages instantly, out of order
        onSystemMessage(msg).map { _ =>
          broadcast(msg)
          Done
        }
    }
  }

  private def markSnapshotAligned(inputIndex: Int,
                                  snapshotId: SnapshotId,
                                  msg: SystemMessage[_ <: Command]): Unit = {
    snapshotAlign match {
      case Some(align) =>
        align.aligned(inputIndex) = true
      case None =>
        val aligned = new Array[Boolean](inputs.size)
        aligned(inputIndex) = true
        snapshotAlign = Some(SnapshotAlign(snapshotId, msg, aligned))
    }
  }

  private def markRestoreAligned(inputIndex: Int,
                                 snapshotId: SnapshotId,
                                 msg: SystemMessage[_ <: Command]): Unit = {
    restoreAlign match {
      case Some(align) =>
        align.aligned(inputIndex) = true
      case None =>
        val aligned = new Array[Boolean](inputs.size)
        aligned(inputIndex) = true
        restoreAlign = Some(SnapshotAlign(snapshotId, msg, aligned))
    }
  }

  private def forEachAsync[T](seq: TraversableOnce[T])(
      process: T => Future[Done]): Future[Done] = {
    seq.foldLeft[Future[Done]](Future.successful(Done)) {
      case (f, i) =>
        f.flatMap { _ =>
          process(i)
        }
    }
  }

  private def broadcast(msg: Message[Out]): Unit = {
    outputs.foreach(_.push(msg))
  }

  override def restorableComponent = this

  override def snapshotState(snapshotId: SnapshotId,
                             snapshotTime: DateTime): Future[State[In, Out]] = {
    val opState = op match {
      case r: Snapshottable =>
        r.snapshotState(snapshotId, snapshotTime).map(state => Some(state))
      case _ =>
        Future.successful(None)
    }

    opState.map { state =>
      State[In, Out](
        alignedQueues.map(_.toVector),
        op.inputs.map(_.snapshotState()).toVector,
        op.outputs.map(_.snapshotState()).toVector,
        state
      )
    }
  }

  override def restoreState(state: State[In, Out]): Future[Unit] = sync {
    alignedQueues.zipWithIndex.foreach {
      case (q, i) =>
        q.clear()
        q.enqueue(state.alignedQueues(i): _*)
    }

    op.inputs.zipWithIndex.foreach {
      case (in, i) =>
        in.restoreState(state.operatorInputs(i))
    }

    op.outputs.zipWithIndex.foreach {
      case (out, i) =>
        out.restoreState(state.operatorOutputs(i))
    }

    op match {
      case r: Snapshottable =>
        r.restoreState(state.operatorState.get.asInstanceOf[r.Snapshot])
      case _ =>
        None
    }
  }
}

object MessageOperator {

  private case class SnapshotAlign(snapshotId: SnapshotId,
                                   originalMessage: SystemMessage[_ <: Command],
                                   aligned: Array[Boolean]) {
    def isComplete = aligned.forall(_ == true)
  }

  case class State[In, Out](alignedQueues: Vector[Vector[In]],
                            operatorInputs: Vector[OperatorInput.State[In]],
                            operatorOutputs: Vector[OperatorOutput.State[Out]],
                            operatorState: Option[Any])

}
