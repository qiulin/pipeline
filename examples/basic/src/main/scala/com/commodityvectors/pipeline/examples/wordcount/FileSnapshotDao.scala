package com.commodityvectors.pipeline.examples.wordcount

import java.nio.charset.StandardCharsets
import java.nio.file.Path

import scala.concurrent.{Future, blocking}
import scala.concurrent.ExecutionContext.Implicits.global

import akka.Done
import org.apache.commons.io.FileUtils
import org.apache.commons.lang3.SerializationUtils

import com.commodityvectors.pipeline.state._

class FileSnapshotDao(dir: Path) extends SnapshotDao {

  dir.toFile.mkdirs()

  override def truncate(): Future[Done] = sync {
    FileUtils.deleteDirectory(dir.toFile)
    Done
  }

  override def findMetadata(
      snapshotId: SnapshotId): Future[Option[SnapshotMetadata]] = sync {
    val file = snapshotMetadataPath(snapshotId).toFile
    if (file.exists()) {
      val data = FileUtils.readFileToByteArray(file)
      val snapshot = SerializationUtils.deserialize[SnapshotMetadata](data)
      Some(snapshot)
    } else {
      None
    }
  }

  override def findLatestMetadata(): Future[Option[SnapshotMetadata]] = {
    val file = latestPath.toFile
    if (file.exists()) {
      sync(FileUtils.readFileToString(file, StandardCharsets.UTF_8)).flatMap {
        snapshotId =>
          findMetadata(SnapshotId(snapshotId))
      }
    } else Future.successful(None)
  }

  override def writeMetadata(snapshot: SnapshotMetadata): Future[Done] = sync {
    val file = snapshotMetadataPath(snapshot.id).toFile
    println(s"Writing a snapshot metadata: ${file.toString}")
    file.getParentFile.mkdirs()

    val data = SerializationUtils.serialize(snapshot)
    FileUtils.writeByteArrayToFile(file, data)

    val latestFile = latestPath.toFile
    println(s"Updating snapshot index: ${latestFile.toString}")
    FileUtils.writeStringToFile(latestFile,
                                snapshot.id.value,
                                StandardCharsets.UTF_8)

    Done
  }

  override def writeData[T <: Serializable](
      snapshotId: SnapshotId,
      componentId: ComponentId,
      data: T): Future[ComponentSnapshotMetadata] = sync {
    val file = snapshotComponentPath(snapshotId, componentId).toFile
    println(s"Writing a snapshot for component: ${file.toString}")
    file.getParentFile.mkdirs()

    val bytes = SerializationUtils.serialize(data)
    FileUtils.writeByteArrayToFile(file, bytes)
    ComponentSnapshotMetadata(componentId)
  }

  override def readData[T <: Serializable](
      snapshotId: SnapshotId,
      componentId: ComponentId): Future[T] = sync {
    val file = snapshotComponentPath(snapshotId, componentId).toFile
    val bytes = FileUtils.readFileToByteArray(file)
    SerializationUtils.deserialize[T](bytes)
  }

  private def sync[T](code: => T): Future[T] = Future {
    blocking {
      code
    }
  }

  private def latestPath: Path = {
    dir.resolve(s"latest")
  }

  private def snapshotPath(snapshotId: SnapshotId): Path = {
    dir.resolve(s"$snapshotId")
  }

  private def snapshotMetadataPath(snapshotId: SnapshotId): Path = {
    snapshotPath(snapshotId).resolve(s"metadata")
  }

  private def snapshotComponentPath(snapshotId: SnapshotId,
                                    componentId: ComponentId): Path = {
    snapshotPath(snapshotId).resolve(s"components/$componentId")
  }
}
