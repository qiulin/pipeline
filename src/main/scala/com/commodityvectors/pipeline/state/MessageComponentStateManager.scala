package com.commodityvectors.pipeline.state

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.language.postfixOps

import akka.Done
import com.typesafe.scalalogging.LazyLogging
import org.joda.time.DateTime

import com.commodityvectors.pipeline.ComponentContext
import com.commodityvectors.pipeline.actors.SnapshotCollectorActor
import com.commodityvectors.pipeline.protocol.SerializedActorRef
import com.commodityvectors.pipeline.util.ActorRefSerialization

trait MessageComponentStateManager {

  def saveComponentState[S <: Serializable](
      state: S,
      componentId: ComponentId,
      snapshotId: SnapshotId,
      snapshotTime: DateTime,
      snapshotCollector: SerializedActorRef): Future[Done]

  def loadComponentState[S <: Serializable](componentId: ComponentId,
                                            snapshotId: SnapshotId): Future[S]
}

class MessageComponentStateManagerImpl(implicit context: ComponentContext)
    extends MessageComponentStateManager
    with ActorRefSerialization
    with LazyLogging {
  private implicit def system = context.system

  def saveComponentState[S <: Serializable](
      state: S,
      componentId: ComponentId,
      snapshotId: SnapshotId,
      snapshotTime: DateTime,
      snapshotCollector: SerializedActorRef): Future[Done] = {
    context.snapshotDao
      .writeData(snapshotId, componentId, state)
      .map { metadata =>
        val collectorRef = deserializeActorRef(snapshotCollector)
        collectorRef ! SnapshotCollectorActor.SnapshotComponentState(metadata)
        Done
      }
      .recoverWith {
        case err: Throwable =>
          logger.error(
            s"Error saving state of a component. componentId = $componentId")
          Future.failed(err)
      }
  }

  def loadComponentState[S <: Serializable](
      componentId: ComponentId,
      snapshotId: SnapshotId): Future[S] = {
    context.snapshotDao
      .readData(snapshotId, componentId)
      .recoverWith {
        case err: Throwable =>
          logger.error(
            s"Error deserializing state of a component. componentId = $componentId")
          Future.failed(err)
      }
  }
}
