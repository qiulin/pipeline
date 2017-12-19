package com.commodityvectors.pipeline.examples.wordcount

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.scaladsl.Source
import com.commodityvectors.pipeline.predefined.StreamReader

class SentenceReader()(implicit system: ActorSystem) extends StreamReader[String] {

  import SentenceReader._

  override type Snapshot = SentenceReaderState

  private val sentences = Vector(
    "Some sentence 1",
    "Some sentence 2",
    "And more",
    "And more",
  )

  override protected def source(state: Option[Snapshot]): Source[(String, Snapshot), NotUsed] = {
    val startFrom = state.map(_.sentencesRead).getOrElse(0)
    val source = Source(sentences.drop(startFrom))
    source.zipWithIndex.map {
      case (s, i) => (s, SentenceReaderState(startFrom + i.toInt))
    }
  }
}

object SentenceReader {

  case class SentenceReaderState(sentencesRead: Int)

}