package com.commodityvectors

import scala.language.implicitConversions

import com.commodityvectors.pipeline.stream.StreamExtensions

package object pipeline extends StreamExtensions {

  type Message[A] = protocol.Message[A]
  val Message = protocol.Message

  type Restorable[S <: Serializable] = state.Restorable[S]
  type Snapshottable = state.Snapshottable

  type SnapshotId = state.SnapshotId
  val SnapshotId = state.SnapshotId

  type ComponentId = state.ComponentId
  val ComponentId = state.ComponentId
}
