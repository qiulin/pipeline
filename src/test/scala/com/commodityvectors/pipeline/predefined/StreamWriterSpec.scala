package com.commodityvectors.pipeline.predefined

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.scaladsl.{Flow, Source}
import org.joda.time.DateTime
import org.scalatest.{AsyncWordSpec, BeforeAndAfterAll, Matchers}

import com.commodityvectors.pipeline.DataComponentContext
import com.commodityvectors.pipeline.DataComponentContext.DefaultDataComponentContext
import com.commodityvectors.pipeline.state.SnapshotId

class StreamWriterSpec
    extends AsyncWordSpec
    with Matchers
    with BeforeAndAfterAll {

  implicit val system = ActorSystem()

  override def afterAll(): Unit = {
    system.terminate()
  }

  class IntWriter extends StreamWriter[Int]() {

    override type Snapshot = Option[Int]

    override protected def initialState(
        context: DataComponentContext): Option[Int] = None

    override protected def flow(
        state: Option[Int]): Flow[Int, Option[Int], NotUsed] = {
      Flow[Int].map(i => Some(i + 1))
    }
  }

  "StreamWriter" when {

    "just initialized " should {

      "save non null snapshot" in {
        val writer = new IntWriter
        for {
          _ <- writer.init(DefaultDataComponentContext("123"))
          snapshot <- writer.snapshotState(SnapshotId("snapshot1"),
                                           DateTime.now)
        } yield {
          snapshot should not be null
        }
      }
    }
  }
}
