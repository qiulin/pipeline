package com.commodityvectors.pipeline.stream

import com.commodityvectors.pipeline.actors.MessageComponentActor
import com.commodityvectors.pipeline.state.MessageComponentStateManagerImpl
import com.commodityvectors.pipeline.stream.operator.OperatorGraphStage
import com.commodityvectors.pipeline.{
  ComponentContext,
  DataOperator,
  Message,
  MessageComponentEndpoint,
  MessageOperator
}

object DataOperatorGraphStage {
  def apply[In, Out](operator: DataOperator[In, Out], id: String)(
      implicit context: ComponentContext)
    : OperatorGraphStage[Message[In], Message[Out]] = {
    implicit def system = context.system

    implicit def coordinator = context.coordinator

    val messageOperator = new MessageOperator[In, Out](operator, id)(
      new MessageComponentStateManagerImpl)
    MessageComponentEndpoint(
      messageOperator,
      MessageComponentActor.props(messageOperator, coordinator.actorPath))
      .start()
    new OperatorGraphStage[Message[In], Message[Out]](messageOperator)
  }
}
