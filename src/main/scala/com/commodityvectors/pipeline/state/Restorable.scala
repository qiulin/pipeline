package com.commodityvectors.pipeline.state

import scala.concurrent.Future

import org.joda.time.DateTime

trait Restorable[S <: Serializable] extends Snapshottable {
  type Snapshot = S
}

trait Snapshottable {

  type Snapshot <: Serializable

  def snapshotState(snapshotId: SnapshotId,
                    snapshotTime: DateTime): Future[Snapshot]

  def restoreState(state: Snapshot): Future[Unit]
}
