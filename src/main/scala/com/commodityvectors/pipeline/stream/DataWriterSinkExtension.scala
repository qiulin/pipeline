package com.commodityvectors.pipeline.stream

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

import akka.Done
import akka.stream.scaladsl._

import com.commodityvectors.pipeline.actors.MessageComponentActor
import com.commodityvectors.pipeline.protocol._
import com.commodityvectors.pipeline.state.MessageComponentStateManagerImpl
import com.commodityvectors.pipeline.{
  ComponentContext,
  DataWriter,
  MessageComponentEndpoint,
  MessageWriter
}

class DataWriterSinkExtension(val src: Sink.type) extends AnyVal {

  def fromDataWriter[A](writer: DataWriter[A], id: String)(
      implicit context: ComponentContext): Sink[Message[A], Future[Done]] = {
    implicit def system = context.system

    implicit def coordinator = context.coordinator

    val messageWriter =
      new MessageWriter(writer, id)(new MessageComponentStateManagerImpl)

    MessageComponentEndpoint(
      messageWriter,
      MessageComponentActor.props(messageWriter, coordinator.actorPath)).start()

    Flow[Message[A]].async
      .mapAsync(1)(m => messageWriter.writeMessage(m))
      .toMat(Sink.ignore)(Keep.right)
      .mapMaterializedValue { done =>
        done.andThen {
          case _ =>
            writer.close()
        }
      }
  }
}
