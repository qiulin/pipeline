package com.commodityvectors.pipeline.exceptions

import com.commodityvectors.pipeline.state.SnapshotId

/**
  * Describes a snapshot loading problem.
  *
  * @param snapshotId id of a snapshot
  * @param cause error that caused it
  */
case class SaveSnapshotException(snapshotId: SnapshotId, cause: Throwable)
    extends Throwable(s"Cannot save a snapshot ($snapshotId)", cause)
