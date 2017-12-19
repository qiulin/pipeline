package com.commodityvectors.pipeline.actors

import akka.actor.{ActorPath, Props}
import com.commodityvectors.pipeline.MessageReader
import com.commodityvectors.pipeline.protocol._

/**
  * Communication endpoint for MessageReader.
  */
class MessageReaderActor[A](reader: MessageReader[A],
                            coordinatorPath: ActorPath)
    extends MessageComponentActor(reader, coordinatorPath) {

  import MessageReaderActor._

  override def ready: Receive = {
    case InjectMessage(msg) => reader.injectMessage(msg)
  }
}

object MessageReaderActor {
  def props[A](reader: MessageReader[A], coordinatorPath: ActorPath) =
    Props(new MessageReaderActor[A](reader, coordinatorPath))

  case class InjectMessage(message: SystemMessage[_ <: Command])

}
