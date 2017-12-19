package com.commodityvectors.pipeline.util

import java.util.concurrent.Executor

class DirectExecutor extends Executor {
  override def execute(command: Runnable): Unit = {
    command.run()
  }
}
