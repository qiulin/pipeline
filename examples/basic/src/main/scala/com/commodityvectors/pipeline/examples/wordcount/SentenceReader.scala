package com.commodityvectors.pipeline.examples.wordcount

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.scaladsl.Source

import com.commodityvectors.pipeline.DataComponentContext
import com.commodityvectors.pipeline.predefined.StreamReader

class SentenceReader()(implicit system: ActorSystem)
    extends StreamReader[String] {

  import SentenceReader._

  override type Snapshot = SentenceReaderState

  private val sentences = (1 to 1000).map(i => s"Some sentence $i")

  override protected def initialState(
      context: DataComponentContext): SentenceReaderState =
    SentenceReaderState(0)

  override protected def source(state: SentenceReaderState)
    : Source[(String, SentenceReaderState), NotUsed] = {
    val startFrom = state.sentencesRead
    val source = Source(sentences.drop(startFrom))
    source.zipWithIndex.map {
      case (s, i) => (s, SentenceReaderState(startFrom + i.toInt))
    }
  }
}

object SentenceReader {

  case class SentenceReaderState(sentencesRead: Int)

}
