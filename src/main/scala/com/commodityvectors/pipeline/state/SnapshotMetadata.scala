package com.commodityvectors.pipeline.state

import org.joda.time.DateTime

case class SnapshotMetadata(
    id: SnapshotId,
    createdAt: DateTime,
    componentSnapshots: Vector[ComponentSnapshotMetadata])
