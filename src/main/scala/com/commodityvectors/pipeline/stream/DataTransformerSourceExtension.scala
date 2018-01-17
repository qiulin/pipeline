package com.commodityvectors.pipeline.stream

import akka.stream.scaladsl.Source

import com.commodityvectors.pipeline.actors.MessageComponentActor
import com.commodityvectors.pipeline.protocol._
import com.commodityvectors.pipeline.state.MessageComponentStateManagerImpl
import com.commodityvectors.pipeline.{
  ComponentContext,
  DataTransformer,
  MessageComponentEndpoint,
  MessageTransformer
}

class DataTransformerSourceExtension[A, Mat](
    val source: Source[Message[A], Mat])
    extends AnyVal {
  def viaDataTransformer[B](transformer: DataTransformer[A, B], id: String)(
      implicit context: ComponentContext): source.ReprMat[Message[B], Mat] = {
    implicit def system = context.system

    implicit def coordinator = context.coordinator

    val messageTransformer = new MessageTransformer(transformer, id)(
      new MessageComponentStateManagerImpl)
    MessageComponentEndpoint(
      messageTransformer,
      MessageComponentActor.props(messageTransformer, coordinator.actorPath))
      .start()
    source.async
      .mapAsync(1)(m => messageTransformer.transformMessage(m))
      .async // allow next transform being called before all resulted messages are processed
      .mapConcat(identity)
  }
}
