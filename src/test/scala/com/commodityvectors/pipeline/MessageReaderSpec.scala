package com.commodityvectors.pipeline

import scala.concurrent.{Await, _}
import scala.concurrent.duration._
import scala.language.postfixOps

import java.util.UUID

import akka.Done
import org.joda.time.DateTime

import com.commodityvectors.pipeline.helpers._
import com.commodityvectors.pipeline.protocol._
import com.commodityvectors.pipeline.state.MessageComponentStateManager

class MessageReaderSpec extends BaseComponentSpec {

  import scala.concurrent.ExecutionContext.Implicits.global

  "MessageReader" should {

    "not read anything until initialized" in {
      val stateManager = mock[MessageComponentStateManager]
      val dataReader = mock[DataReader[Int]]
      val reader = new MessageReader[Int](dataReader, "reader1")(stateManager)

      try {
        // inject to test that even injected messages are not processed
        reader.injectMessage(
          Message.system(RestoreSnapshot(SnapshotId("snapshot1"))))

        val result = reader.readMessage()

        intercept[TimeoutException] {
          Await.result(result, 200 millis)
        }

        (dataReader.close _).expects()
      } finally {
        reader.close()
      }
    }

    "not read user data until data reader is initialized" in {
      val stateManager = mock[MessageComponentStateManager]
      val dataReader = mock[DataReader[Int]]
      val reader = new MessageReader[Int](dataReader, "reader1")(stateManager)

      try {
        reader.initialize(UUID.randomUUID())

        (dataReader.fetch _).expects().never()

        val next = reader.readMessage()

        intercept[TimeoutException] {
          Await.result(next, 200 millis)
        }

        (dataReader.close _).expects()
      } finally {
        reader.close()
      }
    }

    "read user data" in {
      val stateManager = mock[MessageComponentStateManager]
      val dataReader = mock[DataReader[Int]]
      val reader = new MessageReader[Int](dataReader, "reader1")(stateManager)

      try {
        reader.initialize(UUID.randomUUID())
        reader.injectMessage(Message.system(Init()))

        (dataReader.init _).expects().returns(Future.successful())
        (dataReader.fetch _).expects().returns(Future.successful(1))
        (dataReader.pull _).expects().returns(Some(10))

        reader.readMessage().futureValue shouldBe Some(Message.system(Init()))
        reader.readMessage().futureValue shouldBe Some(Message.user(10))

        (dataReader.close _).expects()
      } finally {
        reader.close()
      }
    }

    "stop reading on fetch returned 0" in {
      val stateManager = mock[MessageComponentStateManager]
      val dataReader = mock[DataReader[Int]]
      val reader = new MessageReader[Int](dataReader, "reader1")(stateManager)

      try {
        reader.initialize(UUID.randomUUID())
        reader.injectMessage(Message.system(Init()))

        (dataReader.init _).expects().returns(Future.successful())

        (dataReader.fetch _).expects().returns(Future.successful(1))
        (dataReader.pull _).expects().returns(Some(10))

        (dataReader.fetch _).expects().returns(Future.successful(0))
        (dataReader.close _).expects()

        reader.readMessage().futureValue shouldBe Some(Message.system(Init()))
        reader.readMessage().futureValue shouldBe Some(Message.user(10))
        reader.readMessage().futureValue shouldBe None
        reader.readMessage().futureValue shouldBe None
      } finally {
        reader.close()
      }
    }

    "fail stream on user data error" in {
      val stateManager = mock[MessageComponentStateManager]
      val dataReader = mock[DataReader[Int]]
      val reader = new MessageReader[Int](dataReader, "reader1")(stateManager)
      val error = new Exception("Some error")

      try {
        reader.initialize(UUID.randomUUID())
        reader.injectMessage(Message.system(Init()))

        (dataReader.init _).expects().returns(Future.successful())
        (dataReader.fetch _).expects().returns(Future.failed(error))

        reader.readMessage().futureValue shouldBe Some(Message.system(Init()))
        reader.readMessage().failed.futureValue shouldBe error

        (dataReader.close _).expects()
      } finally {
        reader.close()
      }
    }

    "inject system message even if already waiting for user message " in {
      val stateManager = mock[MessageComponentStateManager]
      val dataReader = mock[DataReader[Int]]
      val reader = new MessageReader[Int](dataReader, "reader1")(stateManager)

      try {
        reader.initialize(UUID.randomUUID())
        reader.injectMessage(Message.system(Init()))

        val userMessagePromise = Promise[Int]()
        (dataReader.init _).expects().returns(Future.successful())
        (dataReader.fetch _).expects().returns(userMessagePromise.future)

        reader.readMessage().futureValue shouldBe Some(Message.system(Init()))

        val nextMessageFuture = reader.readMessage()

        // inject system message after readMessage() has been called
        val snapshotMessage = Message.system(
          CreateSnapshot(SnapshotId("snapId"),
                         DateTime.now,
                         SerializedActorRef("sss")))
        reader.injectMessage(snapshotMessage)

        // user message promise is not resolved, so system message should arrive first
        nextMessageFuture.futureValue shouldBe Some(snapshotMessage)

        (dataReader.close _).expects()
      } finally {
        reader.close()
      }
    }

    "create a snapshot on snapshot message" in {

      val snapshotId = SnapshotId("s1")
      val snapshotDate = DateTime.now
      val collectorRef = SerializedActorRef("collector")

      val stateManager = mock[MessageComponentStateManager]
      val dataReader = mock[CheckpointedDataReader[Int, helpers.TestState]]
      val reader = new MessageReader[Int](dataReader, "reader1")(stateManager)
      try {
        val snapshotMessage =
          Message.system(CreateSnapshot(snapshotId, snapshotDate, collectorRef))
        reader.initialize(UUID.randomUUID())
        reader.injectMessage(snapshotMessage)

        val state = TestState(123)
        (dataReader.snapshotState _)
          .expects(snapshotId, snapshotDate)
          .returns(Future.successful(state))
        (stateManager.saveComponentState _)
          .expects(state,
                   reader.componentId,
                   snapshotId,
                   snapshotDate,
                   collectorRef)
          .returns(Future.successful(Done))

        reader.readMessage().futureValue shouldBe Some(snapshotMessage)

        (dataReader.close _).expects()
      } finally {
        reader.close()
      }
    }

    "restore a snapshot on restore message" in {
      val snapshotId = SnapshotId("s1")

      val stateManager = mock[MessageComponentStateManager]
      val dataReader = mock[CheckpointedDataReader[Int, helpers.TestState]]
      val reader = new MessageReader[Int](dataReader, "reader1")(stateManager)

      try {
        val restoreMessage = Message.system(RestoreSnapshot(snapshotId))
        reader.initialize(UUID.randomUUID())
        reader.injectMessage(restoreMessage)

        val state = TestState(123)

        (stateManager.loadComponentState[helpers.TestState] _)
          .expects(reader.componentId, snapshotId)
          .returns(Future.successful(state))
        (dataReader.restoreState _)
          .expects(state)
          .returns(Future.successful(()))
        (dataReader.init _).expects().returns(Future.successful())

        reader.readMessage().futureValue shouldBe Some(restoreMessage)

        (dataReader.close _).expects()
      } finally {
        reader.close()
      }
    }
  }
}
