package com.commodityvectors.pipeline

import akka.actor.ActorPath
import com.typesafe.config.{Config, ConfigFactory}

case class CoordinatorRefSettings(coordinatorPath: ActorPath)

object CoordinatorRefSettings {
  def load(): CoordinatorRefSettings = load(ConfigFactory.load())

  def load(config: Config): CoordinatorRefSettings = {
    val section = config.getConfig("com.commodityvectors.pipeline.coordinator")
    val path = ActorPath.fromString(section.getString("path"))

    CoordinatorRefSettings(path)
  }
}
