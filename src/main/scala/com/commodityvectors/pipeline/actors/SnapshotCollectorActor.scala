package com.commodityvectors.pipeline.actors

import akka.actor.{Actor, ActorRef, Props}
import org.joda.time.DateTime

import com.commodityvectors.pipeline.state.{
  ComponentId,
  ComponentSnapshotMetadata,
  SnapshotId,
  SnapshotMetadata
}

/**
  * Collects snapshot parts from components and saves it.
  *
  * @param manager            snapshot manager actor ref
  * @param snapshotId         snapshot id to collect
  * @param snapshotTime       snapshot timestamp
  * @param requiredComponents components that must report before snapshot is considered full
  */
class SnapshotCollectorActor(manager: ActorRef,
                             snapshotId: SnapshotId,
                             snapshotTime: DateTime,
                             requiredComponents: List[ComponentId])
    extends Actor {

  import SnapshotCollectorActor._

  private var componentSnapshots: Map[ComponentId, ComponentSnapshotMetadata] =
    Map.empty

  override def receive: Receive = {
    case SnapshotComponentState(snapshot) =>
      componentSnapshots += (snapshot.componentId -> snapshot)
      if (requiredComponents.forall(c => componentSnapshots.contains(c))) {
        val fullSnapshot = SnapshotMetadata(snapshotId,
                                            snapshotTime,
                                            componentSnapshots.values.toVector)
        manager ! SnapshotManagerActor.SnapshotCompleted(fullSnapshot)
        // my mission is complete
        context.stop(self)
      }
  }
}

object SnapshotCollectorActor {

  def props(manager: ActorRef,
            snapshotId: SnapshotId,
            snapshotTime: DateTime,
            requiredComponents: List[ComponentId]) =
    Props(
      new SnapshotCollectorActor(manager,
                                 snapshotId,
                                 snapshotTime,
                                 requiredComponents))

  case class SnapshotComponentState(snapshot: ComponentSnapshotMetadata)

}
