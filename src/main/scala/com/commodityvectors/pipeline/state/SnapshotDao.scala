package com.commodityvectors.pipeline.state

import scala.concurrent.Future

import akka.Done

trait SnapshotDao {

  def truncate(): Future[Done]

  def findMetadata(snapshotId: SnapshotId): Future[Option[SnapshotMetadata]]

  def findLatestMetadata(): Future[Option[SnapshotMetadata]]

  def writeMetadata(snapshot: SnapshotMetadata): Future[Done]

  def writeData[T <: Serializable](snapshotId: SnapshotId,
                                   componentId: ComponentId,
                                   data: T): Future[ComponentSnapshotMetadata]

  def readData[T <: Serializable](snapshotId: SnapshotId,
                                  componentId: ComponentId): Future[T]
}
