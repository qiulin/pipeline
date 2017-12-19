package com.commodityvectors.pipeline.util

import scala.collection.{immutable, mutable}

/**
  * Fast way to generate immutable collections.
  *
  * @param initialSize
  * @tparam A
  */
class Buffer[A](initialSize: Int = 16, initialData: TraversableOnce[A] = Nil) {

  import Buffer._

  private var buffer = new mutable.ArrayBuffer[A](initialSize)

  buffer.appendAll(initialData)

  def size: Int = buffer.size

  def add(elem: A): Unit = {
    buffer += elem
  }

  def clear(): immutable.Seq[A] = {
    val curData = buffer

    // create new mutable buffer
    buffer = new mutable.ArrayBuffer[A](initialSize)

    // return immutable view
    ImmutableSeqWrapper(curData)
  }

  def toVector: Vector[A] = {
    buffer.toVector
  }
}

object Buffer {

  private case class ImmutableSeqWrapper[A](collection: Seq[A])
      extends immutable.Seq[A] {

    override def length: Int = collection.length

    override def apply(idx: Int): A = collection(idx)

    override def iterator: Iterator[A] = collection.iterator
  }

}
