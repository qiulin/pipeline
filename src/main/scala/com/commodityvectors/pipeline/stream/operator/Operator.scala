package com.commodityvectors.pipeline.stream.operator

import scala.collection.immutable
import scala.concurrent.Future

import akka.Done

trait Operator[In, Out] {

  def inputs: immutable.Seq[OperatorInput[In]]
  def outputs: immutable.Seq[OperatorOutput[Out]]

  def run(): Future[Done]
}
