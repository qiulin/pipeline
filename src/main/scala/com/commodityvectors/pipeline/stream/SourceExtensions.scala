package com.commodityvectors.pipeline.stream

import akka.stream.scaladsl.Source

import com.commodityvectors.pipeline.Message

class SourceExtensions[A, Mat](val source: Source[A, Mat]) extends AnyVal {
  def toMessageSource: Source[Message[A], Mat] = {
    source.map(data => Message.user(data))
  }
}
