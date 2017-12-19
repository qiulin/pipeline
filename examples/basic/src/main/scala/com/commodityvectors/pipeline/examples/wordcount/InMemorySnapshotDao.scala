package com.commodityvectors.pipeline.examples.wordcount

import scala.concurrent.Future
import scala.util.Try

import akka.Done
import com.commodityvectors.pipeline.state.{ComponentSnapshotMetadata, SnapshotDao, SnapshotMetadata}
import com.commodityvectors.pipeline.{ComponentId, SnapshotId}

class InMemorySnapshotDao extends SnapshotDao {

  private var snapshotsData = Map.empty[(SnapshotId, ComponentId), Any]
  private var snapshotsMetadata = Map.empty[SnapshotId, SnapshotMetadata]

  override def truncate(): Future[Done] = Future.fromTry {
    Try {
      snapshotsData = Map.empty[(SnapshotId, ComponentId), Any]
      snapshotsMetadata = Map.empty[SnapshotId, SnapshotMetadata]
      Done
    }
  }

  override def findMetadata(snapshotId: SnapshotId): Future[Option[SnapshotMetadata]] = Future.fromTry {
    Try {
      snapshotsMetadata.get(snapshotId)
    }
  }

  override def findLatestMetadata(): Future[Option[SnapshotMetadata]] = Future.fromTry {
    Try {
      snapshotsMetadata.lastOption.map(_._2)
    }
  }

  override def writeMetadata(snapshot: SnapshotMetadata): Future[Done] = Future.fromTry {
    Try {
      snapshotsMetadata += snapshot.id -> snapshot
      Done
    }
  }

  override def writeData[T <: Serializable](snapshotId: SnapshotId, componentId: ComponentId, data: T): Future[ComponentSnapshotMetadata] = Future.fromTry {
    Try {
      println(s"Making a snapshot: $snapshotId, $componentId, $data")
      snapshotsData += (snapshotId, componentId) -> data
      ComponentSnapshotMetadata(componentId)
    }
  }

  override def readData[T <: Serializable](snapshotId: SnapshotId, componentId: ComponentId): Future[T] = Future.fromTry {
    Try {
      snapshotsData((snapshotId, componentId)).asInstanceOf[T]
    }
  }
}