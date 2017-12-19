package com.commodityvectors.pipeline.util

import scala.concurrent.{Future, Promise}

import java.util

/**
  * Concurrent queue that returns future values
  *
  * @tparam A type of elements
  */
class FutureQueue[A] {
  private val queue: util.ArrayDeque[A] = new util.ArrayDeque[A]()
  private val requestsQueue: util.ArrayDeque[Promise[A]] =
    new util.ArrayDeque[Promise[A]]()

  def dequeue(): Future[A] = {
    val promise = Promise[A]()

    queue.synchronized {
      val elem = queue.poll()
      if (elem != null) {
        promise.success(elem)
      } else {
        requestsQueue.add(promise)
      }
    }

    promise.future
  }

  def enqueue(elem: A): Unit = {
    queue.synchronized {
      queue.add(elem)

      val request = requestsQueue.poll()
      if (request != null) {
        val elem = queue.poll()

        request.success(elem)
      }
    }
  }

  def size: Int = queue.size

  def isEmpty: Boolean = queue.isEmpty

  def nonEmpty: Boolean = !isEmpty
}
