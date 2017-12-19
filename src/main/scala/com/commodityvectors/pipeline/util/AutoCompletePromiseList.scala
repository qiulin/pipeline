package com.commodityvectors.pipeline.util

import scala.concurrent.Promise
import scala.util.Try

class AutoCompletePromiseList[A] {

  import AutoCompletePromiseList._
  import ExecutionContexts.Implicits.sameThreadExecutionContext

  private object lock

  private var head: Option[Node[A]] = None
  private var tail: Option[Node[A]] = None

  private val finalPromise: Promise[A] = Promise()

  private def addNode(node: Node[A]) = lock.synchronized {
    for (t <- tail) {
      t.next = Some(node)
    }

    if (head.isEmpty) {
      head = Some(node)
    }

    tail = Some(node)
  }

  private def removeNode(node: Node[A]) = lock.synchronized {
    for (p <- node.prev) {
      p.next = node.next
    }

    for (n <- node.next) {
      n.prev = node.prev
    }

    if (node.prev.isEmpty) {
      head = node.next
    }

    if (node.next.isEmpty) {
      tail = node.prev
    }
  }

  private def iterateNodes(f: Node[A] => Unit) = lock.synchronized {
    var node = head
    while (node.nonEmpty) {
      node match {
        case Some(n) =>
          f(n)
          node = n.next
        case _ =>
      }
    }
  }

  private def register(promise: Promise[A]): Unit = {
    val node = new Node(tail, None, promise)

    addNode(node)

    promise.future.onComplete { _ =>
      removeNode(node)
    }
  }

  def create(): Promise[A] = {
    if (finalPromise.isCompleted) {
      finalPromise
    } else {
      val promise = Promise[A]()
      register(promise)
      promise
    }
  }

  def complete(result: Try[A]): Unit = {
    finalPromise.tryComplete(result)
    iterateNodes(_.data.tryComplete(result))
  }
}

object AutoCompletePromiseList {

  class Node[A](var prev: Option[Node[A]],
                var next: Option[Node[A]],
                val data: Promise[A])

}
