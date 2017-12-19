package com.commodityvectors.pipeline.protocol

import org.joda.time.DateTime

import com.commodityvectors.pipeline.state.SnapshotId

/**
  * Base for all system commands.
  */
sealed trait Command

case class Init() extends Command

case class CreateSnapshot(snapshotId: SnapshotId,
                          snapshotTime: DateTime,
                          collector: SerializedActorRef)
    extends Command

case class RestoreSnapshot(snapshotId: SnapshotId) extends Command
