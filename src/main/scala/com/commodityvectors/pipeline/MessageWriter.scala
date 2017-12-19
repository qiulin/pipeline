package com.commodityvectors.pipeline

import scala.concurrent.Future

import akka.Done

import com.commodityvectors.pipeline.state.MessageComponentStateManager

private class MessageWriter[A](writer: DataWriter[A], id: String)(
    stateManager: MessageComponentStateManager)
    extends MessageComponent(writer, id)(stateManager) {

  override def componentType = MessageComponentType.Writer

  def writeMessage(message: Message[A]): Future[Done] = {

    message match {
      case msg @ Message.system(_, cmd) =>
        onSystemMessage(msg)
      case Message.user(_, data) =>
        writer.write(data)
    }
  }

  def close() = writer.close()
}
