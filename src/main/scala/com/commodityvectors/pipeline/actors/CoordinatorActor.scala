package com.commodityvectors.pipeline.actors

import scala.concurrent.duration.FiniteDuration

import java.util.UUID

import akka.actor.{Actor, ActorLogging, ActorRef, Props}

import com.commodityvectors.pipeline.MessageComponentType.MessageComponentType
import com.commodityvectors.pipeline.protocol.Command
import com.commodityvectors.pipeline.state.{
  ComponentId,
  SnapshotDao,
  SnapshotQuery
}
import com.commodityvectors.pipeline.util.ActorRefSerialization
import com.commodityvectors.pipeline.{
  CoordinatorSettings,
  MessageComponentType,
  protocol
}

/**
  * Data coordinator actor controls the flow of system messages through the stream.
  * Initialization/snapshot notifications are part of it.
  */
class CoordinatorActor(settings: CoordinatorSettings, snapshotDao: SnapshotDao)
    extends Actor
    with ActorLogging
    with ActorRefSerialization {

  import CoordinatorActor._

  // coordinator unique id
  private val uid = UUID.randomUUID()

  log.info(s"Initializing Coordinator($uid)..")

  // primary sources
  private var readers: Map[ComponentId, Option[ActorRef]] =
    settings.readers.map(id => id -> None).toMap

  // a list of all components
  private var components: Map[ComponentId, ActorRef] = Map.empty

  // state flags
  private var isReady: Boolean = false
  private var isStarted: Boolean = false
  private var isReadyToStart: Boolean = false
  private var restoreOptions: Option[SnapshotQuery] = None

  override def receive: Receive = {
    case m @ Start(restoreOpts) =>
      restoreOptions = restoreOpts
      isReadyToStart = true
      startIfReady()

    case HandshakeRequest(componentId, componentType) =>
      sender() ! HandshakeResponse(uid)
      handleHandshake(componentId, componentType, sender())
  }

  def started: Receive = {
    case Broadcast(cmd) =>
      broadcastCommand(cmd)
  }

  private def handleHandshake(componentId: ComponentId,
                              componentType: MessageComponentType,
                              actorRef: ActorRef) = {
    // update component registration table and look for conflicts
    components.get(componentId) match {
      case Some(ref) if ref != actorRef =>
        log.error(s"Component name conflict: $componentId")
      case _ =>
        components += (componentId -> actorRef)
    }

    // update readers registration table
    if (readers.contains(componentId)) {
      readers += (componentId -> Some(actorRef))

      if (componentType != MessageComponentType.Reader) {
        log.warning(
          s"A component '$componentId' registered as a Reader, but reports '$componentType'.")
      }
    } else {
      if (componentType == MessageComponentType.Reader) {
        log.warning(
          s"A Reader component '$componentId' is not registered in coordinator.")
      }
    }

    // when readers are initialized
    if (!isReady) {
      if (readers.values.forall(_.isDefined)) {
        isReady = true

        log.debug("Coordinator is ready.")
        if (isReadyToStart) {
          startIfReady()
        }
      }
    }
  }

  private def startIfReady() = {
    if (isReady && !isStarted) {
      isStarted = true
      context.become(started)

      log.info(s"Coordinator($uid) is started.")

      // start snapshot manager
      def startSnapshotManager(delay: FiniteDuration): ActorRef = {
        context.actorOf(
          SnapshotManagerActor.props(self,
                                     settings.writers,
                                     delay,
                                     settings.snapshotInterval,
                                     snapshotDao),
          "snapshot_manager"
        )
      }

      restoreOptions match {
        case None =>
          // when start normally - send Init command
          startSnapshotManager(settings.snapshotInitialDelay)
          broadcastCommand(protocol.Init())
        case Some(opts) =>
          // when restoring - send RestoreSnapshot command through SnapshotManager
          startSnapshotManager(settings.snapshotInterval) ! SnapshotManagerActor
            .RestoreSnapshot(opts)
      }
    }
  }

  private def broadcastCommand(cmd: protocol.Command) = {
    val readerRefs = readers.values.flatMap(_.toList)
    for (r <- readerRefs) {
      r ! MessageReaderActor.InjectMessage(protocol.Message.system(cmd))
    }
  }
}

object CoordinatorActor {

  def props(settings: CoordinatorSettings, dao: SnapshotDao) =
    Props(new CoordinatorActor(settings, dao))

  // messages

  case class Start(restore: Option[SnapshotQuery] = None)

  case class Broadcast(command: Command)

  case class HandshakeRequest(componentId: ComponentId,
                              componentType: MessageComponentType)

  case class HandshakeResponse(coordinatorUid: UUID)

}
