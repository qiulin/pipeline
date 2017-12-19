package com.commodityvectors.pipeline.stream

import akka.stream.scaladsl.{Flow, Keep, Sink}

import com.commodityvectors.pipeline.Message

class SinkExtensions[A, Mat](val sink: Sink[A, Mat]) extends AnyVal {
  def toMessageSink: Sink[Message[A], Mat] = {
    Flow[Message[A]].mapConcat(_.data.toList).toMat(sink)(Keep.right)
  }
}
