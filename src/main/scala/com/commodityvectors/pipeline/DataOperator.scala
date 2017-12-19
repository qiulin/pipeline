package com.commodityvectors.pipeline

import scala.collection.immutable
import scala.concurrent.Future

import akka.Done

import com.commodityvectors.pipeline.stream.operator.{
  Operator,
  OperatorInput,
  OperatorOutput
}

abstract class DataOperator[In, Out](inputN: Int, outputN: Int)
    extends DataComponent
    with Operator[In, Out] {

  private val _inputs = Vector.tabulate(inputN)(i => new OperatorInput[In](i))
  private val _outputs =
    Vector.tabulate(outputN)(i => new OperatorOutput[Out](i))

  def inputs: immutable.Seq[OperatorInput[In]] = _inputs
  def outputs: immutable.Seq[OperatorOutput[Out]] = _outputs

  def run(): Future[Done]
}
