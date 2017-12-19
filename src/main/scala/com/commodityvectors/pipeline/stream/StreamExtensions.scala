package com.commodityvectors.pipeline.stream

import scala.language.implicitConversions

import akka.NotUsed
import akka.stream.scaladsl.{Flow, Sink, Source}

import com.commodityvectors.pipeline.protocol.Message

trait StreamExtensions {

  implicit def dataReaderSourceExtension(
      src: Source.type): DataReaderSourceExtension =
    new DataReaderSourceExtension(src)

  implicit def dataTransformerFlowExtension[A, B, Mat](
      flow: Flow[Message[A], Message[B], Mat])
    : DataTransformerFlowExtension[A, B, Mat] =
    new DataTransformerFlowExtension(flow)

  implicit def dataTransformerSourceExtension[A, Mat](
      source: Source[Message[A], Mat]): DataTransformerSourceExtension[A, Mat] =
    new DataTransformerSourceExtension(source)

  implicit def dataWriterSinkExtension[A, B, Mat](
      sink: Sink.type): DataWriterSinkExtension =
    new DataWriterSinkExtension(sink)

  implicit def messageFlowExtension[A, B, Mat](
      flow: Flow[Message[A], Message[B], Mat])
    : MessageFlowExtensions[A, B, Mat] =
    new MessageFlowExtensions[A, B, Mat](flow)

  implicit def persistableFlowExtension[A, B <: Serializable, Mat](
      flow: Flow[Message[A], Message[B], Mat])
    : PersistableFlowExtension[A, B, Mat] =
    new PersistableFlowExtension[A, B, Mat](flow)

  implicit def sinkExtensions[A, Mat](
      sink: Sink[A, Mat]): SinkExtensions[A, Mat] =
    new SinkExtensions[A, Mat](sink)

  implicit def sourceExtensions[A, Mat](
      source: Source[A, Mat]): SourceExtensions[A, Mat] =
    new SourceExtensions[A, Mat](source)

  val DataOperator = DataOperatorGraphStage

  type MessageFlow[A, B, Mat] = Flow[Message[A], Message[B], Mat]

  object MessageFlow {
    def apply[A]: MessageFlow[A, A, NotUsed] = Flow[Message[A]]
  }
}
