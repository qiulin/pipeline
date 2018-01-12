package com.commodityvectors.pipeline

import scala.concurrent.Future
import scala.util.Try

/**
  * Base trait for every data component.
  */
trait DataComponent {

  /**
    * Called once at the beginning of component life-cycle.
    * Note that restoreState method will be called before init if there is a restore process.
    */
  def init(): Future[Unit] = Future.successful()

  /**
    * Returns sync code block result as a Future.
    * For better performance
    * and avoiding unnecessary import of execution context.
    */
  protected def sync[T](code: => T) = Future.fromTry(Try(code))
}
