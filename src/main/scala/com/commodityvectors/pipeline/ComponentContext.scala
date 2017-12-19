package com.commodityvectors.pipeline

import akka.actor.ActorSystem

import com.commodityvectors.pipeline.state.SnapshotDao

trait ComponentContext {
  def system: ActorSystem
  def coordinator: CoordinatorRef
  def snapshotDao: SnapshotDao
}

object ComponentContext {
  private case class SimpleComponentContext(system: ActorSystem,
                                            coordinator: CoordinatorRef,
                                            snapshotDao: SnapshotDao)
      extends ComponentContext

  implicit def defaultImplicit(implicit system: ActorSystem,
                               coordinator: CoordinatorRef,
                               snapshotDao: SnapshotDao): ComponentContext = {
    SimpleComponentContext(system, coordinator, snapshotDao)
  }
}
