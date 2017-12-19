package com.commodityvectors.pipeline.examples.wordcount

import scala.collection.immutable
import scala.concurrent.Future

import akka.actor.ActorSystem
import com.commodityvectors.pipeline.{DataTransformer, SnapshotId, Snapshottable}
import org.joda.time.DateTime

class WordCount()(implicit system: ActorSystem) extends DataTransformer[String, Int] with Snapshottable {

  import WordCount._

  override type Snapshot = WordCountState

  private var counter: Int = 0

  override def transform(elem: String): Future[immutable.Seq[Int]] = sync {
    counter += elem.split(" ").size
    List(counter)
  }

  override def snapshotState(snapshotId: SnapshotId, snapshotTime: DateTime): Future[Snapshot] = sync {
    WordCountState(counter)
  }

  override def restoreState(state: Snapshot): Future[Unit] = sync {
    counter = state.counter
  }
}

object WordCount {

  case class WordCountState(counter: Int)

}