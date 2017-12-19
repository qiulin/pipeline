package com.commodityvectors.pipeline

import scala.concurrent.Future

import java.io.Closeable

trait DataReader[A] extends DataComponent with Closeable {

  /**
    * Fetch data asynchronously.
    * @return Number of data items fetched or 0 to signal stream end
    */
  def fetch(): Future[Int]

  /**
    * Pull the fetched data
    * @return Some data or None
    */
  def pull(): Option[A]

  override def close() = ()
}
