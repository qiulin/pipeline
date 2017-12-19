package com.commodityvectors.pipeline

object MessageComponentType extends Enumeration {
  type MessageComponentType = Value

  val Reader, Writer, Transformer, Operator = Value
}
