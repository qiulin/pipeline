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
  def init(context: DataComponentContext): Future[Unit] = Future.successful()

  /**
    * Returns sync code block result as a Future.
    *
    * Use carefully!
    * Heavy CPU tasks can undermine internal Akka Stream dispatcher performance.
    */
  protected def sync[T](code: => T) = Future.fromTry(Try(code))
}
