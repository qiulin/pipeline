package com.commodityvectors.pipeline.exceptions

import com.commodityvectors.pipeline.state.SnapshotQuery

/**
  * Describes a snapshot loading problem.
  *
  * @param snapshotQuery query of a snapshot
  * @param cause error that caused it
  */
case class RestoreSnapshotException(snapshotQuery: SnapshotQuery,
                                    cause: Throwable)
    extends Throwable(s"Cannot restore a snapshot ($snapshotQuery)", cause)
