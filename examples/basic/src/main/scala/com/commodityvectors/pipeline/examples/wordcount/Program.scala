package com.commodityvectors.pipeline.examples.wordcount

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl._
import com.commodityvectors.pipeline._

object Program extends App {
  implicit val system: ActorSystem = ActorSystem()
  implicit val materializer: ActorMaterializer = ActorMaterializer()
  implicit val dao: InMemorySnapshotDao = new InMemorySnapshotDao
  implicit val coordinator: Coordinator = Coordinator(
    new InMemorySnapshotDao,
    CoordinatorSettings
      .load()
      .withReader("reader")
      .withWriter("writer")
  )

  coordinator.start()

  import system.dispatcher

  Source
    .fromDataReader(new SentenceReader, "reader")
    .viaDataTransformer(new WordCount, "counter")
    .runWith(Sink.fromDataWriter(new DummySink, "writer"))
    .andThen {
      case _ => system.terminate()
    }

}