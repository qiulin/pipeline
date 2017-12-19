package com.commodityvectors.pipeline.actors

import scala.concurrent.duration._
import scala.language.postfixOps

import akka.actor.{Actor, ActorPath, Props}

import com.commodityvectors.pipeline.MessageComponent
import com.commodityvectors.pipeline.util.ActorRefSerialization

/**
  * Base endpoint actor for message component.
  * Does handshaking and switches to ready state.
  * @param component message component
  * @param coordinatorPath coordinator path
  */
class MessageComponentActor(component: MessageComponent,
                            coordinatorPath: ActorPath)
    extends Actor
    with ActorRefSerialization {

  import context.dispatcher

  import MessageComponentActor._

  private val handshakeTimer =
    context.system.scheduler.schedule(0 seconds, 5 seconds, self, HandshakeTick)

  final override def receive: Receive = {
    case HandshakeTick =>
      context.actorSelection(coordinatorPath) ! CoordinatorActor
        .HandshakeRequest(component.componentId, component.componentType)
    case CoordinatorActor.HandshakeResponse(coordinatorUid) =>
      handshakeTimer.cancel()
      context.become(ready)
      component.initialize(coordinatorUid)
  }

  def ready: Receive = PartialFunction.empty
}

object MessageComponentActor {
  def props[A](component: MessageComponent, coordinatorPath: ActorPath) =
    Props(new MessageComponentActor(component, coordinatorPath))

  private case object HandshakeTick

}
