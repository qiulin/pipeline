package com.commodityvectors.pipeline.stream

import scala.concurrent.Future

import akka.stream.scaladsl.Source
import akka.{Done, NotUsed}

import com.commodityvectors.pipeline.actors.MessageReaderActor
import com.commodityvectors.pipeline.protocol._
import com.commodityvectors.pipeline.state.MessageComponentStateManagerImpl
import com.commodityvectors.pipeline.{
  ComponentContext,
  DataReader,
  MessageComponentEndpoint,
  MessageReader
}

class DataReaderSourceExtension(val src: Source.type) extends AnyVal {

  import scala.concurrent.ExecutionContext.Implicits.global

  def fromDataReader[A](reader: DataReader[A], id: String)(
      implicit context: ComponentContext): Source[Message[A], NotUsed] = {
    implicit def system = context.system

    implicit def coordinator = context.coordinator

    val messageReader =
      new MessageReader(reader, id)(new MessageComponentStateManagerImpl)
    MessageComponentEndpoint(
      messageReader,
      MessageReaderActor.props(messageReader, coordinator.actorPath)).start()

    Source
      .unfoldResourceAsync[Message[A], MessageReader[A]](
        () => Future.successful(messageReader),
        r => r.readMessage(),
        r =>
          Future {
            r.close()
            Done
        })
      .async
  }

}
