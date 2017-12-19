package com.commodityvectors.pipeline.stream.operator

import java.util
import scala.collection.JavaConverters._

class OperatorOutput[T](n: Int) {
  import OperatorOutput._

  private val queue = new util.ArrayDeque[T]()

  private[pipeline] def pull(): T = Option(queue.poll()).get

  private[pipeline] def nonEmpty: Boolean = !queue.isEmpty

  private[pipeline] def snapshotState(): State[T] = {
    State[T](queue.asScala.toVector)
  }

  private[pipeline] def restoreState(state: State[T]): Unit = {
    queue.clear()
    queue.addAll(state.elements.asJava)
  }

  def channel: Int = n

  def push(data: T): Unit = queue.add(data)

  override def toString: String = {
    queue.asScala.mkString("OperatorOutput(", ", ", ")")
  }
}

object OperatorOutput {
  case class State[T](elements: Vector[T])
}
