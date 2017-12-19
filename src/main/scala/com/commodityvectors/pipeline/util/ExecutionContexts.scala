package com.commodityvectors.pipeline.util

import scala.concurrent.ExecutionContext

import java.util.concurrent.Executors

private[pipeline] object ExecutionContexts {

  /**
    * This is NOT general purpose EC.
    * Use if you know what are you doing.
    */
  lazy val sameThreadExecutionContext =
    ExecutionContext.fromExecutor(new DirectExecutor)

  /**
    * For blocking I/O inside components.
    * Keep it private, so that it won't leak to data components implementation code,
    * otherwise there is a chance they can deadlock each other
    */
  lazy val blockingIO = ExecutionContext.fromExecutor(
    Executors.newCachedThreadPool(new CustomThreadFactory))

  object Implicits {
    implicit lazy val sameThreadExecutionContext =
      ExecutionContexts.sameThreadExecutionContext
    implicit lazy val blockingIO = ExecutionContexts.blockingIO
  }

}
