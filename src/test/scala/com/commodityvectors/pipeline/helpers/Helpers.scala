package com.commodityvectors.pipeline.helpers

import scala.concurrent.Future

import org.joda.time.DateTime

import com.commodityvectors.pipeline.{DataReader, _}

trait CheckpointedDataReader[A, S <: Serializable]
    extends DataReader[A]
    with StubRestorable[S]

trait CheckpointedDataWriter[A, S <: Serializable]
    extends DataWriter[A]
    with StubRestorable[S]

trait CheckpointedDataTransformer[A, B, S <: Serializable]
    extends DataTransformer[A, B]
    with StubRestorable[S]

case class TestState(value: Int)

trait StubRestorable[S <: Serializable] extends Restorable[S] {
  override def snapshotState(snapshotId: SnapshotId,
                             snapshotTime: DateTime): Future[S] = ???
  override def restoreState(state: S): Future[Unit] = ???
}
