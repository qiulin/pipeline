package com.commodityvectors.pipeline.predefined

import scala.concurrent.Future
import scala.util.Failure

import akka.actor.ActorSystem
import akka.stream.scaladsl.{Flow, Keep, Sink, Source, SourceQueueWithComplete}
import akka.stream.{ActorMaterializer, OverflowStrategy}
import akka.{Done, NotUsed}
import com.typesafe.scalalogging.LazyLogging
import org.joda.time.DateTime

import com.commodityvectors.pipeline.util.ExecutionContexts.Implicits.sameThreadExecutionContext
import com.commodityvectors.pipeline.util.{
  AutoCompletePromiseList,
  ExecutionContexts
}
import com.commodityvectors.pipeline.{
  DataComponentContext,
  DataWriter,
  SnapshotId,
  Snapshottable
}

/**
  * Base async data writer
  *
  * @param system
  * @tparam A
  */
abstract class StreamWriter[A](asyncBatchSize: Int = 1000)(
    implicit system: ActorSystem)
    extends DataWriter[A]
    with Snapshottable
    with LazyLogging {

  /**
    * Initial state
    *
    * @param context
    * @return
    */
  protected def initialState(context: DataComponentContext): Snapshot

  protected def flow(state: Snapshot): Flow[A, Snapshot, NotUsed]

  @volatile
  private var state: Snapshot = _
  private var queue: SourceQueueWithComplete[A] = _
  private val queuePromises: AutoCompletePromiseList[Done] =
    new AutoCompletePromiseList[Done]
  private var substreamCompleted: Future[Option[Snapshot]] = _

  protected implicit def materializer: ActorMaterializer = ActorMaterializer()

  override def init(context: DataComponentContext): Future[Unit] = {
    if (state == null) {
      state = initialState(context)
    }

    startSubstream(state)
  }

  private def startSubstream(state: Snapshot): Future[Unit] = {
    Future {
      // run() may deadlock on AffinityPool if executed on the same thread
      Source
        .queue[A](asyncBatchSize, OverflowStrategy.backpressure)
        .via(flow(state))
        .toMat(Sink.lastOption)(Keep.both)
        .run() match {
        case (q, f) =>
          queue = q
          substreamCompleted = f
          substreamCompleted.onComplete {
            case Failure(err) =>
              logger.error(
                s"${this.getClass.getSimpleName} substream stopped with error: " + err)
              queuePromises.complete(Failure(err))
            case _ =>
          }
      }

    }(ExecutionContexts.blockingIO)
  }

  override def write(elem: A): Future[Done] = {
    import ExecutionContexts.Implicits.sameThreadExecutionContext

    val queueOffer = queue.offer(elem).map(_ => Done)

    val queuePromise = queuePromises.create()
    queuePromise.completeWith(queueOffer)
    queuePromise.future
  }

  override def snapshotState(snapshotId: SnapshotId,
                             snapshotTime: DateTime): Future[Snapshot] = {
    // terminate substream gracefully
    queue.complete()
    // and wait for it to finish processing of pending messages
    substreamCompleted
      .flatMap { snapshotOpt =>
        // update state
        state = snapshotOpt.getOrElse(state)
        // restart substream
        startSubstream(state).map { _ =>
          // return last state
          state
        }
      }
  }

  override def restoreState(state: Snapshot): Future[Unit] = sync {
    this.state = state
  }
}
