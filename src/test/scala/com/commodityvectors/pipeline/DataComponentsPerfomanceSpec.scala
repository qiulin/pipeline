package com.commodityvectors.pipeline

import scala.concurrent._
import scala.concurrent.duration._

import akka.Done
import akka.stream.{ActorMaterializer, ActorMaterializerSettings}
import akka.stream.scaladsl.{Sink, Source}

import com.commodityvectors.pipeline.state.SnapshotDao

class DataComponentsPerfomanceSpec extends BaseComponentSpec {

  override implicit val materializer = ActorMaterializer(
    ActorMaterializerSettings(system)
      .withAutoFusing(false)
      .withInputBuffer(4, 32))(system)

  "Benchmark" ignore {
    class TestDataTransformer extends DataTransformer[Int, String] {
      override def transform(elem: Int): Future[List[String]] = sync {
        List(elem.toString)
      }
    }

    class TestDataReader(data: Iterable[Int]) extends DataReader[Int] {
      val dataIterator = data.toIterator

      val pullQueue = collection.mutable.Queue[Int]()

      override def fetch(): Future[Int] = sync {
        if (dataIterator.hasNext) {
          pullQueue.enqueue(dataIterator.next())
          1
        } else 0
      }

      override def pull(): Option[Int] = {
        if (pullQueue.nonEmpty) {
          val a = pullQueue.dequeue()
          Some(a)
        } else {
          None
        }
      }
    }

    class TestDataWriter() extends DataWriter[String] {
      override def write(elem: String): Future[Done] = sync {
        Done
      }
    }

    implicit val snapshotDao = mock[SnapshotDao]

    implicit val coordinator = Coordinator(
      snapshotDao,
      CoordinatorSettings()
        .withReader("reader1")
        .withWriter("writer1")
    )

    coordinator.start()

    val data = (1 to 1000000).toVector

    def run(): Future[Done] = {

      val reader = new TestDataReader(data)
      val transformer = new TestDataTransformer
      val writer = new TestDataWriter

      Source
        .fromDataReader(reader, "reader1")
        .viaDataTransformer(transformer, "transform1")
        .runWith(Sink.fromDataWriter(writer, "writer1"))
    }

    def runEtalon(): Future[Done] = {

      import com.commodityvectors.pipeline.util.ExecutionContexts.Implicits.sameThreadExecutionContext

      val reader = new TestDataReader(data)
      val transformer = new TestDataTransformer
      val writer = new TestDataWriter

      Source
        .unfoldResourceAsync[Int, DataReader[Int]](
          () => Future.successful(reader),
          _ => reader.fetch().map(_ => reader.pull()),
          _ => Future.successful(Done))
        .mapAsync(1)(i => transformer.transform(i))
        .mapConcat(_.toList)
        .mapAsync(1)(i => writer.write(i))
        .runWith(Sink.ignore)
    }

    // should be around 3 sec for 1000000 elements
    measure {
      Await.result(run(), 60 seconds)
    }
  }
}
