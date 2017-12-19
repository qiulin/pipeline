package com.commodityvectors.pipeline.util

import com.commodityvectors.pipeline.BaseComponentSpec

class FutureQueueSpec extends BaseComponentSpec {

  "FutureQueue" should {

    "resolve element before it has been polled" in {
      val queue = new FutureQueue[Int]()

      queue.enqueue(1)
      queue.enqueue(2)

      queue.dequeue().futureValue shouldBe 1
      queue.dequeue().futureValue shouldBe 2
    }

    "resolve element after it has been polled" in {
      val queue = new FutureQueue[Int]()
      val futureElement = queue.dequeue()

      futureElement.isCompleted shouldBe false

      queue.enqueue(1)

      futureElement.futureValue shouldBe 1
    }

    "benchmark" ignore {
      val queue = new FutureQueue[Int]()

      measure {
        for (i <- 1 to 10000000) {
          queue.enqueue(1)
          queue.dequeue()
        }
      }
    }
  }
}
