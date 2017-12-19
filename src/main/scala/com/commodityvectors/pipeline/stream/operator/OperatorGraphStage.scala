package com.commodityvectors.pipeline.stream.operator

import scala.collection.immutable.Seq
import scala.collection.mutable

import akka.stream._
import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler, OutHandler}

import com.commodityvectors.pipeline.util
import com.typesafe.scalalogging.LazyLogging

class OperatorGraphStage[In, Out](operator: Operator[In, Out])
    extends GraphStage[OperatorShape[In, Out]]
    with LazyLogging {

  override val shape =
    OperatorShape[In, Out](operator.inputs.length, operator.outputs.length)

  def inlets: Seq[Inlet[In]] = shape.inlets

  def in(n: Int): Inlet[In] = shape.inlets(n)

  def outlets: Seq[Outlet[Out]] = shape.outlets

  def out(n: Int): Outlet[Out] = shape.outlets(n)

  def trace(s: => String): Unit = {
    logger.trace(s)
  }

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) with SendLogic with OperatorLogic {

      // inbound queues to keep data until operator is ready to take more
      private val inQueues =
        Vector.tabulate(inlets.length)(_ => new mutable.Queue[In]())

      // when all inputs are closed
      private var isCompleting = false

      // when operator flushed the data
      private var isCompleted = false

      // input handlers
      for (i <- inlets.indices) {
        setHandler(
          in(i),
          new InHandler {

            override def onPush(): Unit = {
              trace(s"onPush($i)")
              // enqueue data that we receive
              inQueues(i).enqueue(grab(in(i)))

              // try to run operator if it's not busy
              tryRunOperator()
            }

            override def onUpstreamFinish(): Unit = {
              trace(s"onUpstreamFinish($i)")
              if (inlets.forall(in => isClosed(in))) {
                isCompleting = true
                tryRunOperator()
              }
            }
          }
        )
      }

      def tryRunOperator(fill: Boolean = true) = {
        trace(
          s"tryRunOperator: isDownstreamAwaiting = $isDownstreamAwaiting, isOperatorAwaiting = $isOperatorAwaiting,")
        // no need to run operator if downstream has still enough data or operator is busy itself
        if (isOperatorAwaiting && isDownstreamAwaiting) {
          if (fill) {
            fillOperatorInputs()
          }
          runOperator(isCompleting)
          isCompleted = isCompleting
        }
      }

      /**
        * Fills operator inputs from inbound queue
        * @return true if inputs were modified
        */
      def fillOperatorInputs(): Boolean = {
        var filled = false
        for (i <- inlets.indices) {
          if (isClosed(in(i))) {
            operator.inputs(i).close()
          }

          if (operator.inputs(i).isEmpty && inQueues(i).nonEmpty) {
            operator.inputs(i).push(inQueues(i).dequeue())
            filled = true
          }
        }
        filled
      }

      def publishOperatorOutputs() = {
        for (i <- operator.outputs.indices) {
          while (operator.outputs(i).nonEmpty) {
            val data = operator.outputs(i).pull()
            send(i, data)
          }
        }
      }

      def onOperatorCompleted(consumed: Int): Unit = {
        trace(
          s"onOperatorCompleted: isDownstreamAwaiting = $isDownstreamAwaiting, isCompleted = $isCompleted")

        // publish results
        publishOperatorOutputs()

        // if downstream is still busy processing data, don't care
        // but if it has requested more - run operator again
        if (isDownstreamAwaiting && !isCompleted) {
          // if downstream is awaiting, then it means operator run resulted in no data outputs

          // if operator has consumed something from it's inputs
          // then we can run it again, to see if it consumes more
          if (consumed > 0) {
            trace("operator.inputs.exists(_.hasBeenPulled)")
            tryRunOperator()
          } else {
            // if it has NOT consumed anything, means it's waiting for more data
            if (fillOperatorInputs()) {
              // we can try to fill its inputs from inbound queue if it has something
              tryRunOperator(false)
            } else if (pullMore()) {
              // or we can pull more data
              // and wait until it arrives
            } else {
              // nothing worked, that most likely due to poor operator implementation
              // e.g. it doesn't consume it's inputs
              logger.warn(
                "Entered unexpected state. This may be due to a bug in operator implementation.")
            }
          }
        } else if (isCompleted) {
          // we are done here
          onComplete()
        } else {
          // if downstream is busy,
          // it will request more data later
        }
      }

      override def onRequestMore(outputIndex: Int): Unit = {
        trace(s"onRequestMore: isCompleting = $isCompleting")
        //pullMore()
        tryRunOperator()
      }

      /**
        * Pulles more data
        * @return true if any inbound queue is in pulled state
        */
      def pullMore(): Boolean = {
        var pulled = false
        trace(s"pullMore: ")
        for (i <- inlets.indices) {
          if (inQueues(i).isEmpty) {
            val inlet = in(i)
            if (!hasBeenPulled(inlet) && !isClosed(inlet)) {
              trace(s"pull($i)")
              pull(inlet)
            }

            pulled |= hasBeenPulled(inlet)
          }
        }
        pulled
      }
    }

  trait SendLogic {
    this: GraphStageLogic =>

    private var completed = false

    def onRequestMore(outputIndex: Int): Unit

    private val outQueues =
      Vector.tabulate(outlets.length)(_ => new mutable.Queue[Out]())

    for (i <- outlets.indices) {
      setHandler(
        out(i),
        new OutHandler {
          override def onPull(): Unit = {
            val outQueue = outQueues(i)
            if (outQueue.isEmpty) {
              onRequestMore(i)
            } else {
              push(out(i), outQueue.dequeue())
            }

            tryCompleteStage()
          }
        }
      )
    }

    def isDownstreamAwaiting: Boolean = outlets.forall(out => isAvailable(out))

    def send(i: Int, elem: Out): Unit = {

      outQueues(i).enqueue(elem)

      if (isAvailable(out(i))) {
        push(out(i), outQueues(i).dequeue())
      }
    }

    private def tryCompleteStage() = {
      if (completed && outQueues.forall(_.isEmpty)) {
        completeStage()
      }
    }

    def onComplete(): Unit = {
      completed = true
      tryCompleteStage()
    }
  }

  trait OperatorLogic {
    this: GraphStageLogic =>

    import util.ExecutionContexts.Implicits.sameThreadExecutionContext

    private val operatorCallback =
      getAsyncCallback[Int](n => operatorCompleted(n))
    private val operatorErrorCallback =
      getAsyncCallback[Throwable](err => failStage(err))
    private var isRunning = false

    def onOperatorCompleted(consumed: Int): Unit

    def isOperatorAwaiting = !isRunning

    def runOperator(flush: Boolean): Unit = {
      if (!isRunning) {
        isRunning = true
        val recordsBefore = operator.inputs.foldLeft(0)((s, i) => s + i.size)
        operator.run().map { _ =>
          val recordsAfter = operator.inputs.foldLeft(0)((s, i) => s + i.size)
          operatorCallback.invoke(recordsBefore - recordsAfter)
        } recover {
          case err =>
            operatorErrorCallback.invoke(err)
        }
      }
    }

    private def operatorCompleted(consumed: Int): Unit = {
      isRunning = false
      onOperatorCompleted(consumed)
    }
  }

}
