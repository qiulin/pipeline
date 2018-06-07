package com.commodityvectors.pipeline.examples.wordcount

import java.nio.file.Paths

import scala.concurrent.duration._
import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl._
import com.commodityvectors.pipeline._
import com.commodityvectors.pipeline.state.SnapshotDao

import scala.io.StdIn

object Program extends App {

  implicit val system: ActorSystem = ActorSystem()
  implicit val materializer: ActorMaterializer = ActorMaterializer()

  implicit val dao: SnapshotDao = new FileSnapshotDao(Paths.get("./snapshots"))
  implicit val coordinator: Coordinator = Coordinator(
    dao,
    CoordinatorSettings
      .load()
      .withReader("reader")
      .withWriter("writer")
      .withSnapshotInterval(10.seconds)
  )

  args match {
    case Array("-r") =>
      coordinator.restore()
    case _ =>
      coordinator.start()
  }

  import system.dispatcher

  Source
    .fromDataReader(new SentenceReader, "reader")
    .viaDataTransformer(new WordCount, "counter")
    .runWith(Sink.fromDataWriter(new DummySink, "writer"))
    .andThen {
      case _ => system.terminate()
    }

  StdIn.readLine(s"""
      |${Console.RED}#################################
      |#    Press ENTER to terminate   #
      |#################################${Console.RESET}
    """.stripMargin)
  system.terminate()
}
