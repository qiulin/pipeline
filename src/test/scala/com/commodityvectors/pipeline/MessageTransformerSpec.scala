package com.commodityvectors.pipeline

import scala.concurrent.Future
import scala.language.postfixOps

import java.util.UUID

import akka.Done
import org.joda.time.DateTime

import com.commodityvectors.pipeline.helpers._
import com.commodityvectors.pipeline.protocol._
import com.commodityvectors.pipeline.state.MessageComponentStateManager

class MessageTransformerSpec extends BaseComponentSpec {
  "MessageTransformer" should {

    "pass through all system messages" in {
      val stateManager = mock[MessageComponentStateManager]
      val dataTransformer = mock[DataTransformer[Int, Int]]
      val transformer =
        new MessageTransformer[Int, Int](dataTransformer, "transformer1")(
          stateManager)

      transformer.initialize(UUID.randomUUID())

      val initMsg = Message.system(Init())
      val snapshotMsg = Message.system(
        CreateSnapshot(SnapshotId("snapshot1"),
                       DateTime.now,
                       SerializedActorRef("12345")))
      val restoreMsg = Message.system(RestoreSnapshot(SnapshotId("snapshot1")))

      (dataTransformer.init _).expects()
      transformer.transformMessage(initMsg).futureValue shouldBe List(initMsg)
      transformer.transformMessage(snapshotMsg).futureValue shouldBe List(
        snapshotMsg)
      transformer.transformMessage(restoreMsg).futureValue shouldBe List(
        restoreMsg)
    }

    "copy message headers" in {
      val stateManager = mock[MessageComponentStateManager]
      val dataTransformer = mock[DataTransformer[Int, Int]]
      val transformer =
        new MessageTransformer[Int, Int](dataTransformer, "transformer1")(
          stateManager)

      transformer.initialize(UUID.randomUUID())

      val headers = MessageHeaders(1234)

      (dataTransformer.transform _)
        .expects(1)
        .returns(Future.successful(List(1, 2)))
      transformer
        .transformMessage(Message.user(headers, 1))
        .futureValue shouldBe List(
        Message.user(headers, 1),
        Message.user(headers, 2)
      )
    }

    "create a snapshot on snapshot message" in {
      val snapshotId = SnapshotId("s1")
      val snapshotDate = DateTime.now
      val collectorRef = SerializedActorRef("collector")

      val stateManager = mock[MessageComponentStateManager]
      val dataTransformer =
        mock[CheckpointedDataTransformer[Int, Int, TestState]]
      val transformer =
        new MessageTransformer[Int, Int](dataTransformer, "transformer1")(
          stateManager)

      val snapshotMessage =
        Message.system(CreateSnapshot(snapshotId, snapshotDate, collectorRef))

      val state = TestState(123)
      (dataTransformer.snapshotState _)
        .expects(snapshotId, snapshotDate)
        .returns(Future.successful(state))
      (stateManager.saveComponentState _)
        .expects(state,
                 transformer.componentId,
                 snapshotId,
                 snapshotDate,
                 collectorRef)
        .returns(Future.successful(Done))

      transformer.transformMessage(snapshotMessage).futureValue shouldBe List(
        snapshotMessage)
    }

    "restore a snapshot on restore message" in {
      val snapshotId = SnapshotId("s1")

      val stateManager = mock[MessageComponentStateManager]
      val dataTransformer =
        mock[CheckpointedDataTransformer[Int, Int, TestState]]

      val transformer =
        new MessageTransformer[Int, Int](dataTransformer, "transformer1")(
          stateManager)

      val restoreMessage = Message.system(RestoreSnapshot(snapshotId))

      val state = TestState(123)
      (stateManager.loadComponentState[helpers.TestState] _)
        .expects(transformer.componentId, snapshotId)
        .returns(Future.successful(state))
      (dataTransformer.init _).expects()
      (dataTransformer.restoreState _)
        .expects(state)
        .returns(Future.successful(()))

      transformer.transformMessage(restoreMessage).futureValue shouldBe List(
        restoreMessage)
    }
  }
}
