package com.commodityvectors.pipeline.predefined

import scala.concurrent.Future

import org.joda.time.DateTime

import com.commodityvectors.pipeline.util.Buffer
import com.commodityvectors.pipeline.{
  DataTransformer,
  SnapshotId,
  Snapshottable
}

class GroupedDataTransformer[A <: Serializable](
    size: Int,
    splitBefore: A => Boolean = (_: A) => false,
    splitAfter: A => Boolean = (_: A) => false)
    extends DataTransformer[A, collection.immutable.Seq[A]]
    with Snapshottable {

  import GroupedDataTransformer._

  override type Snapshot = State[A]

  private var buffer = new Buffer[A](size)

  override def transform(elem: A): Future[List[collection.immutable.Seq[A]]] =
    sync {
      var results: List[collection.immutable.Seq[A]] = Nil

      if (splitBefore(elem)) {
        results :+= buffer.clear()
      }

      buffer.add(elem)

      if (splitAfter(elem) || buffer.size >= size) {
        results :+= buffer.clear()
      }

      results
    }

  override def snapshotState(snapshotId: SnapshotId,
                             snapshotTime: DateTime): Future[State[A]] = sync {
    GroupedDataTransformer.State(buffer.toVector)
  }

  override def restoreState(state: State[A]): Future[Unit] = sync {
    buffer = new Buffer(size, state.buffer)
  }
}

object GroupedDataTransformer {

  case class State[A <: Serializable](buffer: Vector[A])
}
