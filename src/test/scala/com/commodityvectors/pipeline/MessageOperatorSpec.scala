package com.commodityvectors.pipeline

import scala.concurrent.Future

import java.util.UUID

import akka.Done
import org.joda.time.DateTime

import com.commodityvectors.pipeline.protocol._
import com.commodityvectors.pipeline.state.MessageComponentStateManager
import com.commodityvectors.pipeline.stream.operator.{
  OperatorInput,
  OperatorOutput
}

class MessageOperatorSpec extends BaseComponentSpec {

  class SumOperator
      extends DataOperator[Int, Int](2, 1)
      with Restorable[SumOperator.State] {
    val in0 = inputs(0)
    val in1 = inputs(1)
    val out = outputs(0)

    override def run(): Future[Done] = sync {
      if (in0.nonEmpty && in1.nonEmpty) {
        out.push(in0.pull() + in1.pull())
      }
      Done
    }

    override def snapshotState(
        snapshotId: SnapshotId,
        snapshotTime: DateTime): Future[SumOperator.State] = sync {
      SumOperator.State()
    }

    override def restoreState(state: SumOperator.State): Future[Unit] = sync {}
  }

  object SumOperator {

    case class State()

  }

  "MessageOperator" should {

    "align on snapshot" in {

      val coordinatorUid = UUID.randomUUID()
      val snapshotId = SnapshotId("s1")
      val snapshotDate = DateTime.now
      val collectorRef = SerializedActorRef("collector")

      val operator = new SumOperator
      val stateManager = mock[MessageComponentStateManager]
      val messageOperator =
        new MessageOperator[Int, Int](operator, "sum")(stateManager)

      val snapshotMsg =
        Message.system(CreateSnapshot(snapshotId, snapshotDate, collectorRef))

      messageOperator.inputs(0).push(Message.user(11))
      messageOperator.inputs(0).push(Message.user(12))
      messageOperator.inputs(0).push(Message.user(13))
      messageOperator.inputs(0).push(snapshotMsg)

      messageOperator.inputs(1).push(Message.user(21))
      messageOperator.inputs(1).push(snapshotMsg)
      messageOperator.inputs(1).push(Message.user(22))
      messageOperator.inputs(1).push(Message.user(23))

      messageOperator.initialize(coordinatorUid)
      messageOperator.run().futureValue
      messageOperator.run().futureValue
      messageOperator.run().futureValue

      val expectedState = MessageOperator.State[Int, Int](
        Vector(Vector(13), Vector.empty), // aligned queues
        Vector(
          OperatorInput.State[Int](Vector(12), false),
          OperatorInput
            .State[Int](Vector.empty, false)), // data that was in operator queues during alignment
        Vector(OperatorOutput.State[Int](Vector.empty)),
        Some(SumOperator.State())
      )

      (stateManager.saveComponentState[MessageOperator.State[Int, Int]] _)
        .expects(expectedState,
                 messageOperator.componentId,
                 snapshotId,
                 snapshotDate,
                 collectorRef)
        .returns(Future.successful(Done))

      messageOperator.run().futureValue
      messageOperator.run().futureValue
      messageOperator.run().futureValue

      messageOperator.outputs(0).snapshotState() shouldBe OperatorOutput.State(
        Vector(
          Message.user(32),
          snapshotMsg,
          Message.user(34),
          Message.user(36)
        ))
    }

    "restore state" in {
      val coordinatorUid = UUID.randomUUID()
      val snapshotId = SnapshotId("s1")
      val snapshotDate = DateTime.now

      val operator = new SumOperator
      val stateManager = mock[MessageComponentStateManager]
      val messageOperator =
        new MessageOperator[Int, Int](operator, "sum")(stateManager)
      val restoreMsg = Message.system(RestoreSnapshot(snapshotId))
      val savedState = MessageOperator.State[Int, Int](
        Vector(Vector(13), Vector.empty), // aligned queues
        Vector(
          OperatorInput.State[Int](Vector(12), false),
          OperatorInput
            .State[Int](Vector.empty, false)), // data that was in operator queues during alignment
        Vector(OperatorOutput.State[Int](Vector.empty)),
        Some(SumOperator.State())
      )

      messageOperator.inputs(0).push(restoreMsg)

      messageOperator.inputs(1).push(Message.user(11))
      messageOperator.inputs(1).push(restoreMsg)
      messageOperator.inputs(1).push(Message.user(22))
      messageOperator.inputs(1).push(Message.user(23))

      messageOperator.initialize(coordinatorUid)

      (stateManager.loadComponentState[MessageOperator.State[Int, Int]] _)
        .expects(messageOperator.componentId, snapshotId)
        .returns(Future.successful(savedState))

      messageOperator.run().futureValue
      messageOperator.run().futureValue
      messageOperator.run().futureValue
      messageOperator.run().futureValue

      messageOperator.outputs(0).snapshotState() shouldBe OperatorOutput.State(
        Vector(
          restoreMsg,
          Message.user(34),
          Message.user(36)
        ))
    }

    "return error on restore error" in {
      val coordinatorUid = UUID.randomUUID()
      val snapshotId = SnapshotId("s1")

      val operator = new SumOperator
      val stateManager = mock[MessageComponentStateManager]
      val messageOperator =
        new MessageOperator[Int, Int](operator, "sum")(stateManager)
      val restoreMsg = Message.system(RestoreSnapshot(snapshotId))
      val error = new Exception("Fake error")

      messageOperator.inputs(0).push(restoreMsg)
      messageOperator.inputs(1).push(restoreMsg)

      messageOperator.initialize(coordinatorUid)

      (stateManager.loadComponentState[MessageOperator.State[Int, Int]] _)
        .expects(messageOperator.componentId, snapshotId)
        .returns(Future.failed(error))

      messageOperator.run().failed.futureValue shouldBe error
    }

    "keep backpressure" in {
      val operator = new SumOperator
      val stateManager = mock[MessageComponentStateManager]
      val messageOperator =
        new MessageOperator[Int, Int](operator, "sum")(stateManager)

      messageOperator.inputs(0).push(Message.user(11))
      messageOperator.inputs(0).push(Message.user(12))
      messageOperator.inputs(0).push(Message.user(13))

      messageOperator.run().futureValue
      messageOperator.run().futureValue
      messageOperator.run().futureValue

      // message operator should not pull its inputs if date operator has not pulled its corresponding inputs
      messageOperator.inputs(0).snapshotState() shouldBe OperatorInput.State(
        Vector(
          Message.user(12),
          Message.user(13)
        ),
        false)
    }
  }
}
