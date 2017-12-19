package com.commodityvectors.pipeline

import akka.actor.{ActorPath}

trait CoordinatorRef {
  def actorPath: ActorPath
}
