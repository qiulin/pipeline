package com.commodityvectors.pipeline.util

import scala.concurrent.Future
import scala.util.Try

object FutureTry {
  def apply[T](code: => T): Future[T] = Future.fromTry(Try(code))
}
