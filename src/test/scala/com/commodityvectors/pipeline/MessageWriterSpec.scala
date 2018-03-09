package com.commodityvectors.pipeline

import scala.concurrent._
import scala.language.postfixOps

import akka.Done
import org.joda.time.DateTime

import com.commodityvectors.pipeline.helpers._
import com.commodityvectors.pipeline.protocol._
import com.commodityvectors.pipeline.state.MessageComponentStateManager

class MessageWriterSpec extends BaseComponentSpec {
  "MessageWriter" should {

    "create a snapshot on snapshot message" in {
      val snapshotId = SnapshotId("s1")
      val snapshotDate = DateTime.now
      val collectorRef = SerializedActorRef("collector")

      val stateManager = mock[MessageComponentStateManager]
      val dataWriter = mock[CheckpointedDataWriter[Int, TestState]]
      val writer = new MessageWriter[Int](dataWriter, "writer1")(stateManager)

      val snapshotMessage =
        Message.system(CreateSnapshot(snapshotId, snapshotDate, collectorRef))

      val state = TestState(123)
      (dataWriter.snapshotState _)
        .expects(snapshotId, snapshotDate)
        .returns(Future.successful(state))
      (stateManager.saveComponentState _)
        .expects(state,
                 writer.componentId,
                 snapshotId,
                 snapshotDate,
                 collectorRef)
        .returns(Future.successful(Done))

      writer.writeMessage(snapshotMessage).futureValue
    }

    "restore a snapshot on restore message" in {
      val snapshotId = SnapshotId("s1")

      val stateManager = mock[MessageComponentStateManager]
      val dataWriter = mock[CheckpointedDataWriter[Int, TestState]]
      val writer = new MessageWriter[Int](dataWriter, "writer1")(stateManager)

      val restoreMessage = Message.system(RestoreSnapshot(snapshotId))

      val state = TestState(123)
      (stateManager.loadComponentState[helpers.TestState] _)
        .expects(writer.componentId, snapshotId)
        .returns(Future.successful(state))
      (dataWriter.init _).expects(*).returns(Future.successful())
      (dataWriter.restoreState _).expects(state).returns(Future.successful(()))

      writer.writeMessage(restoreMessage).futureValue
    }
  }
}
