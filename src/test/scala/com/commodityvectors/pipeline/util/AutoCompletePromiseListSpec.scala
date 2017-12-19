package com.commodityvectors.pipeline.util

import scala.util.Success

import com.commodityvectors.pipeline.BaseComponentSpec

class AutoCompletePromiseListSpec extends BaseComponentSpec {

  "AutoCompletePromiseList" should {

    "complete all promises" in {
      val list = new AutoCompletePromiseList[Int]()

      val p1 = list.create()
      val p2 = list.create()
      val p3 = list.create()

      list.complete(Success(1))

      p1.future.value shouldBe Some(Success(1))
      p2.future.value shouldBe Some(Success(1))
      p3.future.value shouldBe Some(Success(1))
    }

    "be fast" ignore {
      val list = new AutoCompletePromiseList[Int]()

      measure {
        val n = 1000000
        val promises = (1 to n).map(_ => list.create())
        promises.foreach(_.success(1))
      }
    }
  }
}
