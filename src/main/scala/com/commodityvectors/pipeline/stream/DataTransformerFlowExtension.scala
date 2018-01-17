package com.commodityvectors.pipeline.stream

import akka.stream.scaladsl.Flow

import com.commodityvectors.pipeline.actors.MessageComponentActor
import com.commodityvectors.pipeline.protocol.Message
import com.commodityvectors.pipeline.state.MessageComponentStateManagerImpl
import com.commodityvectors.pipeline.{
  ComponentContext,
  DataTransformer,
  MessageComponentEndpoint,
  MessageTransformer
}

class DataTransformerFlowExtension[A, B, Mat](
    val flow: Flow[Message[A], Message[B], Mat])
    extends AnyVal {

  def viaDataTransformer[C](transformer: DataTransformer[B, C], id: String)(
      implicit context: ComponentContext): flow.ReprMat[Message[C], Mat] = {
    implicit def system = context.system
    implicit def coordinator = context.coordinator

    val messageTransformer = new MessageTransformer(transformer, id)(
      new MessageComponentStateManagerImpl)

    MessageComponentEndpoint(
      messageTransformer,
      MessageComponentActor.props(messageTransformer, coordinator.actorPath))
      .start()

    flow.async
      .mapAsync(1)(m => messageTransformer.transformMessage(m))
      .async
      .mapConcat(identity)
  }
}
