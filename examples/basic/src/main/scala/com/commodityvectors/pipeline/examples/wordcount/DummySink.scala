package com.commodityvectors.pipeline.examples.wordcount

import scala.concurrent.Future

import akka.Done
import com.commodityvectors.pipeline.DataWriter

class DummySink() extends DataWriter[Int] {
  override def write(elem: Int): Future[Done] = sync {
    println(s"Count: $elem")
    Done
  }
}
