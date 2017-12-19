package com.commodityvectors.pipeline.state

sealed trait SnapshotQuery

case object LatestSnapshot extends SnapshotQuery

case class CustomSnapshot(snapshotId: SnapshotId) extends SnapshotQuery
