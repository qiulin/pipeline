package com.commodityvectors.pipeline.predefined

import scala.concurrent.Future

import java.util.concurrent.ConcurrentLinkedQueue

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Keep, Sink, SinkQueueWithCancel, Source}
import com.commodityvectors.pipeline._
import com.commodityvectors.pipeline.util.ExecutionContexts
import com.typesafe.scalalogging.LazyLogging
import org.joda.time.DateTime

/**
  * Base Akka stream based data reader
  *
  * @param system actor system
  * @tparam A data type
  */
abstract class StreamReader[A](implicit system: ActorSystem)
    extends DataReader[A]
    with Snapshottable
    with LazyLogging {

  private var state: Snapshot = _
  private var fetchQueue: SinkQueueWithCancel[(A, Snapshot)] = _
  private val pullQueue = new ConcurrentLinkedQueue[(A, Snapshot)]()

  /**
    * Source of data.
    * Each data record should be accompanied with corresponding immutable state.
    *
    * @param state initial state
    * @return
    */
  protected def source(state: Option[Snapshot]): Source[(A, Snapshot), NotUsed]

  protected implicit def materializer: ActorMaterializer = ActorMaterializer()

  override def init(): Future[Unit] = {
    Future {
      // run() may deadlock on AffinityPool if executed on the same thread
      fetchQueue = source(Option(state))
        .toMat(Sink.queue())(Keep.right)
        .run()
    }(ExecutionContexts.blockingIO)
  }

  /**
    * Fetch data asynchronously.
    *
    * @return Number of data items fetched or 0 to signal stream end
    */
  override def fetch(): Future[Int] = {
    import ExecutionContexts.Implicits.sameThreadExecutionContext
    fetchQueue.pull().map {
      case Some(data) =>
        pullQueue.add(data)
        1
      case _ =>
        0
    }
  }

  /**
    * Pull the fetched data
    *
    * @return Some data or None
    */
  override def pull(): Option[A] = {
    pullQueue.poll() match {
      case (a, s) =>
        state = s
        Some(a)
      case _ =>
        None
    }
  }

  override def snapshotState(snapshotId: SnapshotId,
                             snapshotTime: DateTime): Future[Snapshot] = sync {
    state
  }

  override def restoreState(state: Snapshot): Future[Unit] = sync {
    this.state = state
  }

  override def close(): Unit = {
    fetchQueue.cancel()
  }
}
