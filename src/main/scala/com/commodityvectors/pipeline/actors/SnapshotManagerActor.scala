package com.commodityvectors.pipeline.actors

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.language.postfixOps

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import org.joda.time.DateTime

import com.commodityvectors.pipeline.exceptions.{
  RestoreSnapshotException,
  SaveSnapshotException
}
import com.commodityvectors.pipeline.protocol
import com.commodityvectors.pipeline.state._
import com.commodityvectors.pipeline.util.ActorRefSerialization

class SnapshotManagerActor(coordinator: ActorRef,
                           sinks: List[ComponentId],
                           initialDelay: FiniteDuration,
                           interval: FiniteDuration,
                           snapshotDao: SnapshotDao)
    extends Actor
    with ActorLogging
    with ActorRefSerialization {

  import context.dispatcher

  import SnapshotManagerActor._

  context.system.scheduler.schedule(initialDelay,
                                    interval,
                                    self,
                                    CreateSnapshot)

  override def receive: Receive = {
    case CreateSnapshot =>
      createSnapshot()

    case RestoreSnapshot(opts) =>
      restoreSnapshot(opts)

    case SnapshotCompleted(snapshot) =>
      saveSnapshot(snapshot)

    case SnapshotLoaded(snapshot) =>
      restoreSnapshot(snapshot)

    case SnapshotLoadFailed(snapshotQuery, err) =>
      throw RestoreSnapshotException(snapshotQuery, err)

    case SnapshotSaveFailed(snapshot, err) =>
      throw SaveSnapshotException(snapshot.id, err)
  }

  private def generateSnapshotId(): SnapshotId = {
    SnapshotId(DateTime.now.getMillis.toString)
  }

  private def createSnapshot(): Unit = {
    val snapshotId = generateSnapshotId()
    val snapshotTime = DateTime.now
    log.info(s"Creating a snapshot: $snapshotId..")

    val collector = context.actorOf(
      SnapshotCollectorActor.props(self, snapshotId, snapshotTime, sinks))
    coordinator ! CoordinatorActor.Broadcast(
      protocol.CreateSnapshot(snapshotId, snapshotTime, collector))
  }

  private def restoreSnapshot(opts: SnapshotQuery): Unit = {
    log.info(s"Restoring a snapshot: $opts..")

    loadSnapshot(opts).map { snapshot =>
      self ! SnapshotLoaded(snapshot)
    } recover {
      case err =>
        self ! SnapshotLoadFailed(opts, err)
    }
  }

  private def restoreSnapshot(snapshot: SnapshotMetadata): Unit = {
    coordinator ! CoordinatorActor.Broadcast(
      protocol.RestoreSnapshot(snapshot.id))
  }

  private def saveSnapshot(snapshot: SnapshotMetadata): Unit = {
    snapshotDao
      .writeMetadata(snapshot)
      .map { _ =>
        log.info(s"Snapshot(${snapshot.id}) is saved.")
      }
      .recover {
        case err: Throwable =>
          self ! SnapshotSaveFailed(snapshot, err)
      }
  }

  private def loadSnapshot(opts: SnapshotQuery): Future[SnapshotMetadata] = {
    val snapshotOpt = opts match {
      case LatestSnapshot     => snapshotDao.findLatestMetadata()
      case CustomSnapshot(id) => snapshotDao.findMetadata(id)
    }

    snapshotOpt.map {
      case Some(s) => s
      case None =>
        sys.error(s"Cannot find a snapshot: $snapshotOpt")
    }
  }
}

object SnapshotManagerActor {

  def props(coordinator: ActorRef,
            sinks: List[ComponentId],
            initialDeleay: FiniteDuration,
            interval: FiniteDuration,
            snapshotDao: SnapshotDao) =
    Props(
      new SnapshotManagerActor(coordinator,
                               sinks,
                               initialDeleay,
                               interval,
                               snapshotDao))

  case object CreateSnapshot

  case class RestoreSnapshot(query: SnapshotQuery)

  case class SnapshotLoaded(snapshot: SnapshotMetadata)

  case class SnapshotLoadFailed(snapshotQuery: SnapshotQuery, err: Throwable)

  case class SnapshotCompleted(snapshot: SnapshotMetadata)

  case class SnapshotSaveFailed(snapshot: SnapshotMetadata, err: Throwable)

}
