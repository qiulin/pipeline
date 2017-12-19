package com.commodityvectors.pipeline.util

import java.util.concurrent.ForkJoinPool.ForkJoinWorkerThreadFactory
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.{
  Executors,
  ForkJoinPool,
  ForkJoinWorkerThread,
  ThreadFactory
}

/**
  * Creates daemon threads.
  */
class CustomThreadFactory(namePattern: String = "thread-%d",
                          isDaemon: Boolean = true)
    extends ThreadFactory
    with ForkJoinWorkerThreadFactory {

  private val threadNumber = new AtomicLong(0)

  private lazy val defaultThreadFactory = Executors.defaultThreadFactory
  private lazy val defaultForkJoinWorkerThreadFactory =
    ForkJoinPool.defaultForkJoinWorkerThreadFactory

  private def threadName: String =
    namePattern.format(threadNumber.incrementAndGet)

  override def newThread(r: Runnable): Thread = {
    val thread = defaultThreadFactory.newThread(r)
    thread.setDaemon(isDaemon)
    thread.setName(threadName)
    thread
  }

  override def newThread(pool: ForkJoinPool): ForkJoinWorkerThread = {
    val thread = defaultForkJoinWorkerThreadFactory.newThread(pool)
    thread.setDaemon(isDaemon)
    thread.setName(threadName)
    thread
  }
}
