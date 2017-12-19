package com.commodityvectors.pipeline.stream

import akka.stream.scaladsl.Flow

import com.commodityvectors.pipeline.ComponentContext
import com.commodityvectors.pipeline.predefined.GroupedDataTransformer
import com.commodityvectors.pipeline.protocol.Message

class PersistableFlowExtension[A, B <: Serializable, Mat](
    val flow: Flow[Message[A], Message[B], Mat])
    extends AnyVal {
  def groupedData(maxSize: Int,
                  splitBefore: B => Boolean = (_: B) => false,
                  splitAfter: B => Boolean = (_: B) => false)(id: String)(
      implicit context: ComponentContext)
    : Flow[Message[A], Message[collection.immutable.Seq[B]], Mat] = {
    flow.viaDataTransformer(
      new GroupedDataTransformer[B](maxSize, splitBefore, splitAfter),
      id)
  }
}
