package com.commodityvectors.pipeline.predefined

import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{Matchers, WordSpec}

class GroupedDataTransformerSpec
    extends WordSpec
    with Matchers
    with ScalaFutures {

  case class TestData(value: Int) {
    override def toString: String = value.toString
  }

  "GroupedDataTransformer" should {

    "split group on max size" in {
      val data = (1 to 100).map(i => TestData(i)).toVector
      val transformer = new GroupedDataTransformer[TestData](20)

      val expectedGroups = data.grouped(20).toVector
      val groups: Seq[Seq[TestData]] =
        data.flatMap(d => transformer.transform(d).futureValue)

      groups shouldBe expectedGroups
    }

    "split group on 'before' condition" in {
      val data = (1 to 10).map(i => TestData(i)).toVector
      val transformer =
        new GroupedDataTransformer[TestData](200,
                                             splitBefore = _.value % 3 == 0)

      val expectedGroups = ((1 until 3) :: (3 until 6) :: (6 until 9) :: Nil)
        .map(_.map(i => TestData(i)).toVector)
      val groups: Seq[Seq[TestData]] =
        data.flatMap(d => transformer.transform(d).futureValue)

      groups shouldBe expectedGroups
    }

    "split group on 'after' condition" in {
      val data = (1 to 10).map(i => TestData(i)).toVector
      val transformer =
        new GroupedDataTransformer[TestData](200, splitAfter = _.value % 3 == 0)

      val expectedGroups = ((1 to 3) :: (4 to 6) :: (7 to 9) :: Nil)
        .map(_.map(i => TestData(i)).toVector)
      val groups: Seq[Seq[TestData]] =
        data.flatMap(d => transformer.transform(d).futureValue)

      groups shouldBe expectedGroups
    }
  }
}
