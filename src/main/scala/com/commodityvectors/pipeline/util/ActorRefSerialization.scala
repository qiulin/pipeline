package com.commodityvectors.pipeline.util

import akka.actor.{ActorRef, ActorSystem, ExtendedActorSystem, ScalaActorRef}
import akka.serialization.Serialization
import com.commodityvectors.pipeline.protocol.SerializedActorRef

import scala.language.implicitConversions

trait ActorRefSerialization {

  implicit def deserializeActorRef(ref: SerializedActorRef)(
      implicit system: ActorSystem): ActorRef = {
    system.asInstanceOf[ExtendedActorSystem].provider.resolveActorRef(ref.value)
  }

  implicit def serializeActorRef(ref: ActorRef): SerializedActorRef = {
    SerializedActorRef(Serialization.serializedActorPath(ref))
  }
}
