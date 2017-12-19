package com.commodityvectors.pipeline.stream.operator

import scala.collection.immutable
import scala.collection.immutable.Seq

import akka.stream.{Inlet, Outlet, Shape}

case class OperatorShape[In, Out](inlets: immutable.Seq[Inlet[In]],
                                  outlets: immutable.Seq[Outlet[Out]])
    extends Shape {
  override def deepCopy(): Shape =
    OperatorShape(inlets.map(_.carbonCopy()), outlets.map(_.carbonCopy()))

  def in(i: Int): Inlet[In] = inlets(i)

  def out(i: Int): Outlet[Out] = outlets(i)
}

object OperatorShape {
  def apply[In, Out](inN: Int, outN: Int): OperatorShape[In, Out] = {
    val inlets: Vector[Inlet[In]] =
      Vector.tabulate(inN)(i => Inlet[In](s"in_$i"))
    val outlets: Vector[Outlet[Out]] =
      Vector.tabulate(outN)(i => Outlet[Out](s"out_$i"))
    OperatorShape(inlets, outlets)
  }
}
