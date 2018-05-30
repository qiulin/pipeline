package com.commodityvectors.pipeline.predefined

import scala.concurrent.duration._

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.scaladsl.Flow
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

  class IntWriter(saveDelay: FiniteDuration = 1.milli)
      extends StreamWriter[Int]() {

    override type Snapshot = Option[Int]

    override protected def initialState(
        context: DataComponentContext): Option[Int] = None

    override protected def flow(
        state: Option[Int]): Flow[Int, Option[Int], NotUsed] = {
      Flow[Int].map(i => Some(i + 1)).delay(saveDelay)
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

    "has pending messages to be saved" should {

      "wait for them to be saved before making snapshot" in {
        val writer = new IntWriter(1.second)
        for {
          _ <- writer.init(DefaultDataComponentContext("123"))
          _ <- writer.write(1)
          _ <- writer.write(2)
          _ <- writer.write(3)
          snapshot1 <- writer.snapshotState(SnapshotId("snapshot1"),
                                            DateTime.now)
          _ <- writer.write(4)
          _ <- writer.write(5)
          snapshot2 <- writer.snapshotState(SnapshotId("snapshot1"),
                                            DateTime.now)
        } yield {
          snapshot1 shouldBe Some(4)
          snapshot2 shouldBe Some(6)
        }
      }
    }

  }
}
