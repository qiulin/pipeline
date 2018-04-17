package com.commodityvectors.pipeline.predefined

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.scaladsl.Source
import org.joda.time.DateTime
import org.scalatest.{AsyncWordSpec, BeforeAndAfterAll, Matchers}

import com.commodityvectors.pipeline.DataComponentContext
import com.commodityvectors.pipeline.DataComponentContext.DefaultDataComponentContext
import com.commodityvectors.pipeline.state.SnapshotId

class StreamReaderSpec
    extends AsyncWordSpec
    with Matchers
    with BeforeAndAfterAll {

  implicit val system = ActorSystem()

  override def afterAll(): Unit = {
    system.terminate()
  }

  class IntReader extends StreamReader[Int]() {

    override type Snapshot = Option[Int]

    override protected def initialState(
        context: DataComponentContext): Snapshot = None

    override protected def source(
        state: Snapshot): Source[(Int, Snapshot), NotUsed] =
      Source.repeat((1, Some(2)))
  }

  "StreamReader" when {

    "just initialized " should {

      "save non null snapshot" in {
        val reader = new IntReader
        for {
          _ <- reader.init(DefaultDataComponentContext("123"))
          snapshot <- reader.snapshotState(SnapshotId("snapshot1"),
                                           DateTime.now)
        } yield {
          snapshot should not be null
        }
      }
    }
  }
}
