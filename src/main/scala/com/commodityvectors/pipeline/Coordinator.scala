package com.commodityvectors.pipeline

import akka.actor.{ActorPath, ActorSystem}

import com.commodityvectors.pipeline.actors.CoordinatorActor
import com.commodityvectors.pipeline.state.{
  CustomSnapshot,
  LatestSnapshot,
  SnapshotDao
}

/**
  * Wrapper for coordinator actor.
  */
class Coordinator(val settings: CoordinatorSettings, snapshotDao: SnapshotDao)(
    implicit system: ActorSystem)
    extends CoordinatorRef {

  val actorRef =
    system.actorOf(CoordinatorActor.props(settings, snapshotDao), settings.name)

  def start(): Unit = {
    actorRef ! CoordinatorActor.Start()
  }

  def restore(): Unit = {
    actorRef ! CoordinatorActor.Start(Some(LatestSnapshot))
  }

  def restore(snapshotId: SnapshotId): Unit = {
    actorRef ! CoordinatorActor.Start(Some(CustomSnapshot(snapshotId)))
  }

  override def actorPath: ActorPath = actorRef.path
}

object Coordinator {
  def apply(snapshotDao: SnapshotDao)(
      implicit system: ActorSystem): Coordinator =
    new Coordinator(CoordinatorSettings.load(), snapshotDao)

  def apply(snapshotDao: SnapshotDao, settings: CoordinatorSettings)(
      implicit system: ActorSystem): Coordinator =
    new Coordinator(settings, snapshotDao)

  def ref(path: ActorPath): CoordinatorRef = new CoordinatorRef {
    override def actorPath: ActorPath = path
  }

  def ref(path: String): CoordinatorRef = ref(ActorPath.fromString(path))

  def ref(settings: CoordinatorRefSettings): CoordinatorRef =
    ref(settings.coordinatorPath)

  def ref(): CoordinatorRef = ref(CoordinatorRefSettings.load())
}
