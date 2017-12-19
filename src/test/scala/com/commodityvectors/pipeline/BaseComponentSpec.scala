package com.commodityvectors.pipeline

import akka.actor.ActorSystem
import akka.stream.{ActorMaterializer, ActorMaterializerSettings}
import akka.testkit.TestKit
import org.scalamock.scalatest.MockFactory
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpec}
import org.scalatest.concurrent.ScalaFutures

class BaseComponentSpec
    extends WordSpec
    with BeforeAndAfterAll
    with ScalaFutures
    with MockFactory
    with Matchers {

  implicit val system = ActorSystem()
  implicit val materializer = ActorMaterializer(
    ActorMaterializerSettings(system)
      .withInputBuffer(1, 1))

  override def afterAll {
    TestKit.shutdownActorSystem(system)
  }

  def measure[T](code: => T): T = {
    val t1 = System.nanoTime
    val result = code
    val t2 = System.nanoTime
    println(s"${(t2 - t1) / 1000000} ms")
    result
  }
}
