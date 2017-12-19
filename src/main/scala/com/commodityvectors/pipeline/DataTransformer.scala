package com.commodityvectors.pipeline

import scala.collection.immutable
import scala.concurrent.Future

trait DataTransformer[In, Out] extends DataComponent {

  def transform(elem: In): Future[immutable.Seq[Out]]
}
