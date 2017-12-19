package com.commodityvectors.pipeline.stream

import akka.stream.scaladsl.Flow

import com.commodityvectors.pipeline.protocol.Message

class MessageFlowExtensions[A, B, Mat](
    val flow: Flow[Message[A], Message[B], Mat])
    extends AnyVal {

  def filterData(p: B => Boolean) = {
    flow.filter {
      case msg @ Message.system(_, _) => true
      case Message.user(_, data)      => p(data)
    }
  }

  def mapData[C](f: B => C): Flow[Message[A], Message[C], Mat] = {
    flow.map(_.map(f))
  }

  def mapConcatData[C](f: B => collection.immutable.Iterable[C])
    : Flow[Message[A], Message[C], Mat] = {
    flow.mapConcat {
      case msg @ Message.system(_, _) =>
        List(msg)
      case Message.user(headers, data) =>
        f(data).map(d => Message.user(headers, d))
    }
  }

  def collectData[C](
      f: PartialFunction[B, C]): Flow[Message[A], Message[C], Mat] = {
    flow.collect {
      case msg @ Message.system(_, _) =>
        msg
      case msg @ Message.user(headers, data) if f.isDefinedAt(data) =>
        msg.map(f)
    }
  }
}
