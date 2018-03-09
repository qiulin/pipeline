package com.commodityvectors.pipeline

/**
  * Initialization context for DataComponent
  */
trait DataComponentContext {

  /**
    * Identifier assigned to the component
    * @return
    */
  def id: String
}

object DataComponentContext {
  private[pipeline] case class DefaultDataComponentContext(id: String)
      extends DataComponentContext
}
