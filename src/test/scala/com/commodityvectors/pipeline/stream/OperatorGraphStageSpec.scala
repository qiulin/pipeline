package com.commodityvectors.pipeline.stream

import scala.collection.immutable.Seq
import scala.concurrent.Future

import akka.Done
import akka.stream.ClosedShape
import akka.stream.scaladsl.{GraphDSL, RunnableGraph}
import akka.stream.testkit.javadsl.TestSink
import akka.stream.testkit.scaladsl.TestSource

import com.commodityvectors.pipeline.stream.operator.{
  Operator,
  OperatorGraphStage,
  OperatorInput,
  OperatorOutput
}
import com.commodityvectors.pipeline.{BaseComponentSpec, DataOperator}

class OperatorGraphStageSpec extends BaseComponentSpec {

  class MergeOperator[T, K](inputN: Int, sortBy: T => K)(
      implicit ordering: Ordering[K])
      extends DataOperator[T, T](inputN, 1) {
    val output = outputs.head

    override def run(): Future[Done] = sync {
      // while it's more effective to have a loop here,
      // don't use it here to test that it works even without it
      if (inputs.exists(_.nonEmpty) && inputs.forall(
            i => i.nonEmpty || i.isClosed)) {
        for (input <- inputs
               .filter(_.nonEmpty)
               .sortBy(e => sortBy(e.peek()))
               .headOption) {
          val elem = input.pull()
          output.push(elem)
        }
      }

      Done
    }
  }

  def merge_sorted_2_1(operator: MergeOperator[Int, Int] =
    new MergeOperator[Int, Int](2, i => i)) = {
    val source1 = TestSource.probe[Int]
    val source2 = TestSource.probe[Int]
    val sink = TestSink.probe[Int](system)

    val graph = RunnableGraph.fromGraph(
      GraphDSL.create(source1, source2, sink)((_, _, _)) {
        implicit b => (source1, source2, sink) =>
          import GraphDSL.Implicits._

          val merge = b.add(new OperatorGraphStage[Int, Int](operator))

          source1 ~> merge.in(0)
          source2 ~> merge.in(1)

          merge.out(0) ~> sink

          ClosedShape
      })

    graph.run()
  }

  "DataOperator" should {

    "pull different number of elements from sources" in {
      val (sc1, sc2, si) = merge_sorted_2_1()

      si.request(100)

      sc1.sendNext(5)
      sc2.sendNext(1)
      sc2.sendNext(2)
      sc2.sendNext(3)
      sc2.sendNext(4)
      sc2.sendNext(6)

      sc1.sendComplete()
      sc2.sendComplete()

      si.expectNextN(List(1, 2, 3, 4, 5, 6))
      si.expectComplete()
    }

    "complete when both sources are initially empty" in {

      val (sc1, sc2, si) = merge_sorted_2_1()

      si.request(100)
      sc1.sendComplete()
      sc2.sendComplete()
      si.expectComplete()
    }

    "fail when source fails" in {

      val (sc1, sc2, si) = merge_sorted_2_1()

      val error = new Exception("testError")

      si.request(100)
      sc1.sendNext(1)
      sc2.sendError(error)
      si.expectError(error)
    }

    "fail when operator fails" in {

      val error = new Exception("Fake error")

      class FailingOperator extends MergeOperator[Int, Int](2, i => i) {
        override def run(): Future[Done] = Future.failed(error)
      }

      val (sc1, sc2, si) = merge_sorted_2_1(new FailingOperator)
      si.request(100)
      si.expectError(error)
    }

    "request elements only when needs them" in {

      val (sc1, sc2, si) = merge_sorted_2_1()

      si.request(100)

      sc1.sendNext(10)
      sc2.sendNext(1)

      si.expectNext(1)

      sc1.sendNext(11)
      sc2.sendNext(2)

      si.expectNext(2)

      sc1.sendNext(12)
      sc2.sendNext(3)

      si.expectNext(3)

      // 10 is not processed yet, and we also have 11, so source #1 should not be asked for more yet
      sc1.expectNoMsg()
    }

    "run operator when downstream is awaiting but input queues are full" in {
      val operator = new MergeOperator[Int, Int](2, i => i)
      val (sc1, sc2, si) = merge_sorted_2_1(operator)

      for (i <- 1 to 4) {
        operator.inputs(0).push(i)
        operator.inputs(1).push(i)
      }

      si.request(1000)
      for (i <- 1 to 2) {
        sc1.sendNext(i)
        sc2.sendNext(i)
      }

      si.expectNextN(6)
    }
  }
}
