package com.commodityvectors.pipeline.actors

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.language.postfixOps

import akka.actor.{
  Actor,
  ActorRef,
  ActorSystem,
  OneForOneStrategy,
  Props,
  SupervisorStrategy
}
import akka.testkit.{ImplicitSender, TestKit, TestProbe}
import org.joda.time.DateTime
import org.scalamock.scalatest.MockFactory
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}

import com.commodityvectors.pipeline.exceptions.SaveSnapshotException
import com.commodityvectors.pipeline.protocol
import com.commodityvectors.pipeline.state.{
  SnapshotDao,
  SnapshotId,
  SnapshotMetadata
}

class SnapshotManagerActorSpec
    extends TestKit(ActorSystem("test"))
    with ImplicitSender
    with WordSpecLike
    with Matchers
    with MockFactory
    with BeforeAndAfterAll {

  override def afterAll {
    TestKit.shutdownActorSystem(system)
  }

  class StepParent(target: ActorRef) extends Actor {
    override val supervisorStrategy = OneForOneStrategy() {
      case thr: Throwable => {
        target ! thr
        SupervisorStrategy.Stop
      }
    }

    def receive = {
      case p: Props =>
        sender ! context.actorOf(p)
    }
  }

  "SnapshotManagerActor" should {

    "trigger snapshot after delay" in {
      val snapshotDao = mock[SnapshotDao]
      val coordinator = TestProbe("coordinator")

      val delay = 0.seconds
      system.actorOf(
        SnapshotManagerActor
          .props(coordinator.ref, Nil, delay, 1 hour, snapshotDao))

      coordinator.expectMsgPF(delay + 4.seconds) {
        case CoordinatorActor.Broadcast(protocol.CreateSnapshot(_, _, _)) =>
      }
    }

    "fail on snapshot save error" in {
      val snapshotDao = mock[SnapshotDao]
      val coordinator = TestProbe("coordinator")

      val parent = system.actorOf(Props(new StepParent(coordinator.ref)))
      parent ! SnapshotManagerActor.props(coordinator.ref,
                                          Nil,
                                          1 hour,
                                          1 hour,
                                          snapshotDao)
      val actor = expectMsgType[ActorRef]

      val snapshot =
        SnapshotMetadata(SnapshotId("123"), DateTime.now, Vector.empty)
      val error = new Exception("test error")

      (snapshotDao.writeMetadata _)
        .expects(snapshot)
        .returns(Future.failed(error))
        .once()

      actor ! SnapshotManagerActor.SnapshotCompleted(snapshot)

      coordinator.expectMsgType[SaveSnapshotException]
    }
  }
}
