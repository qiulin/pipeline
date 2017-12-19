package com.commodityvectors.pipeline.stream.operator

import scala.concurrent.Future

import akka.Done
import com.commodityvectors.pipeline.DataOperator

case class OperatorTester[In, Out](op: DataOperator[In, Out]) {
  def run(): Future[Done] = {
    op.run()
  }

  def pushInput(i: Int, elem: In): Unit = {
    op.inputs(i).push(elem)
  }

  def pullOutput(i: Int): Out = {
    op.outputs(i).pull()
  }

  def pullOutputOpt(i: Int): Option[Out] = {
    if (op.outputs(i).nonEmpty)
      Some(op.outputs(i).pull())
    else
      None
  }
}
