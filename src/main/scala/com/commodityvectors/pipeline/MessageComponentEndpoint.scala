package com.commodityvectors.pipeline

import akka.actor.{ActorSystem, Props}

/**
  * A network-node for message component, that performs handshaking and initialization of the component
  * @param component message component
  * @param props endpoint actor props
  * @param system actor system
  */
private case class MessageComponentEndpoint(
    component: MessageComponent,
    props: Props)(implicit system: ActorSystem) {
  def start(): Unit = {
    system.actorOf(
      props,
      "message_component_" + component.componentId.value.replace(" ", "_"))
  }
}
