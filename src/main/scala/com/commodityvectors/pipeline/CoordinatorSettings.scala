package com.commodityvectors.pipeline

import java.util.concurrent.TimeUnit

import akka.actor.ActorPath
import com.typesafe.config.{Config, ConfigFactory}
import scala.collection.JavaConversions._
import scala.concurrent.duration._
import scala.language.postfixOps

case class CoordinatorSettings(name: String = "coordinator",
                               readers: List[ComponentId] = Nil,
                               writers: List[ComponentId] = Nil,
                               snapshotInitialDelay: FiniteDuration = 1 hour,
                               snapshotInterval: FiniteDuration = 1 hour) {
  def withName(name: String): CoordinatorSettings = this.copy(name = name)

  def withReader(name: String): CoordinatorSettings =
    this.copy(readers = ComponentId(name) :: readers)

  def withWriter(name: String): CoordinatorSettings =
    this.copy(writers = ComponentId(name) :: writers)

  def withReader(name: ComponentId): CoordinatorSettings =
    this.copy(readers = name :: readers)

  def withWriter(names: ComponentId*): CoordinatorSettings =
    this.copy(writers = names.toList ++ writers)

  def withSnapshotInterval(
      snapshotInterval: FiniteDuration): CoordinatorSettings =
    this.copy(snapshotInterval = snapshotInterval)
}

object CoordinatorSettings {

  def load(): CoordinatorSettings =
    CoordinatorSettings.load(ConfigFactory.load())

  def load(config: Config): CoordinatorSettings = {
    val section = config.getConfig("com.commodityvectors.pipeline.coordinator")
    val path = ActorPath.fromString(section.getString("path"))

    val name = path.name
    val readers = section.getStringList("readers").toList.map(ComponentId.apply)
    val writers = section.getStringList("writers").toList.map(ComponentId.apply)
    val initialDelay =
      section.getDuration("snapshot.initial-delay", TimeUnit.SECONDS).seconds
    val interval =
      section.getDuration("snapshot.interval", TimeUnit.SECONDS).seconds
    CoordinatorSettings(name, readers, writers, initialDelay, interval)
  }
}
