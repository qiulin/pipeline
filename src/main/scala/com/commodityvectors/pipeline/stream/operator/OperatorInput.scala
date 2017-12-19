package com.commodityvectors.pipeline.stream.operator

import java.util
import scala.collection.JavaConverters._

class OperatorInput[T](n: Int) {

  import OperatorInput._

  private val queue = new util.ArrayDeque[T]()

  private var _isClosed: Boolean = false

  private[pipeline] def size: Int = queue.size

  private[pipeline] def close(): Unit = _isClosed = true

  private[pipeline] def push(elem: T): Unit = queue.add(elem)

  private[pipeline] def snapshotState(): State[T] = {
    State[T](queue.asScala.toVector, isClosed)
  }

  private[pipeline] def restoreState(state: State[T]): Unit = {
    queue.clear()
    queue.addAll(state.elements.asJava)
    _isClosed = state.isClosed
  }

  def channel: Int = n

  def peek(): T = Option(queue.peek()).get

  def pull(): T = Option(queue.poll()).get

  def isClosed: Boolean = _isClosed

  def isEmpty: Boolean = queue.isEmpty

  def nonEmpty: Boolean = !isEmpty

  override def toString: String = {
    queue.asScala.mkString("OperatorInput(", ", ", ")")
  }
}

object OperatorInput {
  case class State[T](elements: Vector[T], isClosed: Boolean)
}
