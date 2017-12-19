package com.commodityvectors.pipeline

import scala.concurrent.Future

import java.io.Closeable

import akka.Done

trait DataWriter[A] extends DataComponent with Closeable {

  def write(elem: A): Future[Done]

  override def close() = ()
}
