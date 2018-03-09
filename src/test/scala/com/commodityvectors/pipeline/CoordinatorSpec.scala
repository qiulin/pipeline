package com.commodityvectors.pipeline

import scala.concurrent._
import scala.concurrent.duration._

import java.util.UUID

import akka.Done
import akka.stream.scaladsl.{Sink, Source}
import akka.testkit.TestProbe
import org.joda.time.DateTime

import com.commodityvectors.pipeline.actors.{
  CoordinatorActor,
  MessageReaderActor
}
import com.commodityvectors.pipeline.helpers._
import com.commodityvectors.pipeline.protocol.Init
import com.commodityvectors.pipeline.state.{
  ComponentSnapshotMetadata,
  SnapshotDao,
  SnapshotMetadata
}

class CoordinatorSpec extends BaseComponentSpec {

  def randomName(prefix: String = ""): String = {
    s"$prefix${UUID.randomUUID().toString}"
  }

  "Data coordinator" when {

    "not started" should {

      "block all readers" in {

        val reader = mock[DataReader[Int]]

        val readerName = randomName("reader_")

        implicit val snapshotDao = mock[SnapshotDao]
        implicit val coordinator = Coordinator(
          snapshotDao,
          CoordinatorSettings()
            .withReader(readerName)
        )

        val result = Source
          .fromDataReader(reader, readerName)
          .runWith(Sink.ignore)

        intercept[TimeoutException] {
          Await.result(result, 200 millis)
        }
      }

      "accept start message before handshake finished" in {
        val readerName = ComponentId("reader1")
        implicit val snapshotDao = mock[SnapshotDao]
        val coordinator = system.actorOf(
          CoordinatorActor.props(CoordinatorSettings().withReader(readerName),
                                 snapshotDao))
        val probe = TestProbe()
        implicit val sender = probe.ref

        coordinator ! CoordinatorActor.Start()

        coordinator ! CoordinatorActor.HandshakeRequest(
          readerName,
          MessageComponentType.Reader)

        probe.expectMsgPF() {
          case CoordinatorActor.HandshakeResponse(_) =>
        }

        probe.expectMsgPF() {
          case MessageReaderActor.InjectMessage(Message.system(_, Init())) =>
        }
      }

      "accept start message after handshake finished" in {
        val readerName = ComponentId("reader1")
        implicit val snapshotDao = mock[SnapshotDao]
        val coordinator = system.actorOf(
          CoordinatorActor.props(CoordinatorSettings().withReader(readerName),
                                 snapshotDao))
        val probe = TestProbe()
        implicit val sender = probe.ref

        coordinator ! CoordinatorActor.HandshakeRequest(
          readerName,
          MessageComponentType.Reader)

        probe.expectMsgPF() {
          case CoordinatorActor.HandshakeResponse(_) =>
        }

        coordinator ! CoordinatorActor.Start()

        probe.expectMsgPF() {
          case MessageReaderActor.InjectMessage(Message.system(_, Init())) =>
        }
      }
    }

    "started" should {

      "initialize all components" in {

        val reader = stub[DataReader[Int]]
        (reader.fetch _).when().returns(Future.successful(0))
        (reader.init _).when(*).returns(Future.successful())

        val transformer = stub[DataTransformer[Int, Int]]
        (transformer.init _).when(*).returns(Future.successful())
        (transformer.transform _).when(*).returns(Future.successful(Nil))

        val writer = stub[DataWriter[Int]]
        (writer.init _).when(*).returns(Future.successful())
        (writer.write _).when(*).returns(Future.successful(Done))

        val coordinatorName = randomName("coordinator_")
        val readerName = randomName("reader_")
        val writerName = randomName("writer_")
        val transformName = randomName("transform_")

        implicit val snapshotDao = mock[SnapshotDao]
        implicit val coordinator = Coordinator(
          snapshotDao,
          CoordinatorSettings()
            .withName(coordinatorName)
            .withReader(readerName)
            .withWriter(writerName)
        )

        coordinator.start()

        val result = Source
          .fromDataReader(reader, readerName)
          .viaDataTransformer(transformer, transformName)
          .runWith(Sink.fromDataWriter(writer, writerName))

        whenReady(result) { _ =>
          (reader.init _).verify(*).once()
          (transformer.init _).verify(*).once()
          (writer.init _).verify(*).once()
        }
      }

      "restore components state before init" in {

        val reader = stub[CheckpointedDataReader[Int, helpers.TestState]]
        (reader.fetch _).when().returns(Future.successful(0))
        (reader.restoreState _).when(*).returns(Future.successful(()))
        (reader.init _).when(*).returns(Future.successful())

        val transformer =
          stub[CheckpointedDataTransformer[Int, Int, helpers.TestState]]
        (transformer.init _).when(*).returns(Future.successful())
        (transformer.transform _).when(*).returns(Future.successful(Nil))
        (transformer.restoreState _)
          .when(*)
          .returns(Future.successful(()))

        val writer = stub[CheckpointedDataWriter[Int, helpers.TestState]]
        (writer.init _).when(*).returns(Future.successful())
        (writer.write _).when(*).returns(Future.successful(Done))
        (writer.restoreState _).when(*).returns(Future.successful(()))

        val coordinatorName = randomName("coordinator_")
        val readerName = ComponentId(randomName("reader_"))
        val writerName = ComponentId(randomName("writer_"))
        val transformName = ComponentId(randomName("transform_"))

        val snapshot = SnapshotMetadata(
          SnapshotId("123"),
          DateTime.now,
          Vector(
            ComponentSnapshotMetadata(readerName),
            ComponentSnapshotMetadata(writerName),
            ComponentSnapshotMetadata(transformName)
          )
        )
        implicit val snapshotDao = mock[SnapshotDao]
        (snapshotDao.findMetadata _)
          .expects(snapshot.id)
          .returns(Future.successful(Some(snapshot)))
        (snapshotDao.readData[TestState] _)
          .expects(snapshot.id, readerName)
          .returns(Future.successful(TestState(21)))
        (snapshotDao.readData[TestState] _)
          .expects(snapshot.id, writerName)
          .returns(Future.successful(TestState(21)))
        (snapshotDao.readData[TestState] _)
          .expects(snapshot.id, transformName)
          .returns(Future.successful(TestState(21)))

        implicit val coordinator = Coordinator(
          snapshotDao,
          CoordinatorSettings()
            .withName(coordinatorName)
            .withReader(readerName)
            .withWriter(writerName)
        )

        coordinator.restore(snapshot.id)

        val result = Source
          .fromDataReader(reader, readerName.value)
          .viaDataTransformer(transformer, transformName.value)
          .runWith(Sink.fromDataWriter(writer, writerName.value))

        whenReady(result) { _ =>
          inSequence {
            (reader.restoreState _).verify(*).once()
            (reader.init _).verify(*).once()
          }

          inSequence {
            (transformer.restoreState _).verify(*).once()
            (transformer.init _).verify(*).once()
          }

          inSequence {
            (writer.restoreState _).verify(*).once()
            (writer.init _).verify(*).once()
          }
        }
      }

    }

  }
}
