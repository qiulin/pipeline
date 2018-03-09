package com.commodityvectors.pipeline.predefined

import scala.concurrent.Future
import akka.actor.ActorSystem
import akka.stream.scaladsl.{Flow, Keep, Sink, Source, SourceQueueWithComplete}
import akka.stream.{ActorMaterializer, OverflowStrategy}
import akka.{Done, NotUsed}
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
import com.typesafe.scalalogging.LazyLogging
import org.joda.time.DateTime

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

  protected def flow(state: Option[Snapshot]): Flow[A, Snapshot, NotUsed]

  @volatile
  private var state: Snapshot = _
  private var queue: SourceQueueWithComplete[A] = _
  private val queuePromises: AutoCompletePromiseList[Done] =
    new AutoCompletePromiseList[Done]

  protected implicit def materializer: ActorMaterializer = ActorMaterializer()

  override def init(context: DataComponentContext): Future[Unit] = {
    Future {
      // run() may deadlock on AffinityPool if executed on the same thread
      Source
        .queue[A](asyncBatchSize, OverflowStrategy.backpressure)
        .via(flow(Option(state)))
        .map { s =>
          state = s
        }
        .toMat(Sink.ignore)(Keep.both)
        .run() match {
        case (q, f) =>
          queue = q
          f.onComplete { result =>
            logger.error(
              s"${this.getClass.getSimpleName} substream stopped with result: " + result)
            queuePromises.complete(result)
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
                             snapshotTime: DateTime): Future[Snapshot] = sync {
    state
  }

  override def restoreState(state: Snapshot): Future[Unit] = sync {
    this.state = state
  }
}
