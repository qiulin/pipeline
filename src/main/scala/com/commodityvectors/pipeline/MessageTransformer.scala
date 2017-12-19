package com.commodityvectors.pipeline

import scala.collection.immutable
import scala.concurrent.Future

import com.commodityvectors.pipeline.state.MessageComponentStateManager

private class MessageTransformer[In, Out](
    transformer: DataTransformer[In, Out],
    id: String)(stateManager: MessageComponentStateManager)
    extends MessageComponent(transformer, id)(stateManager) {

  override def componentType = MessageComponentType.Transformer

  def transformMessage(
      message: Message[In]): Future[immutable.Seq[Message[Out]]] = {

    import util.ExecutionContexts.Implicits.sameThreadExecutionContext

    message match {
      case msg @ Message.system(_, _) =>
        onSystemMessage(msg).map(_ => List(msg))
      case Message.user(headers, data) =>
        transformer.transform(data).map(_.map(d => Message.user(headers, d)))
    }
  }
}
