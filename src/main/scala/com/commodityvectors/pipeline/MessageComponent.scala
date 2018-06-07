package com.commodityvectors.pipeline

import scala.concurrent.{Future, Promise}
import scala.language.postfixOps
import java.util.UUID

import akka.Done

import com.commodityvectors.pipeline.MessageComponentType.MessageComponentType
import com.commodityvectors.pipeline.protocol._
import com.commodityvectors.pipeline.state.{
  EmptyState,
  MessageComponentStateManager
}
import com.commodityvectors.pipeline.util.FutureTry

private abstract class MessageComponent(component: DataComponent, id: String)(
    stateManager: MessageComponentStateManager) {
  import scala.concurrent.ExecutionContext.Implicits.global

  private lazy val initializedPromise: Promise[Done] = Promise[Done]
  private var _isComponentInitialized = false

  def componentId: ComponentId = ComponentId(id)

  def componentType: MessageComponentType

  def initialize(coordinatorUid: UUID) = {
    initializedPromise.trySuccess(Done)
  }

  protected final def sync[T](code: => T) = FutureTry(code)

  protected def initialized: Future[Done] = initializedPromise.future

  protected def isComponentInitialized: Boolean = _isComponentInitialized

  /**
    * Reference to a component which state is serialized for snapshot.
    */
  protected def restorableComponent: Any = component

  private def tryInitializeComponent(): Future[Unit] = {
    if (!_isComponentInitialized) {
      val context = DataComponentContext.DefaultDataComponentContext(id)
      component.init(context).map { _ =>
        _isComponentInitialized = true
      }
    } else {
      Future.successful()
    }
  }

  protected def onSystemMessage[A <: Command](
      message: SystemMessage[A]): Future[Done] = message match {
    case Message.system(_, cmd: Init) =>
      onInitializeComponent(cmd)

    case Message.system(_, cmd: CreateSnapshot) =>
      onSnapshotComponent(cmd)

    case Message.system(_, cmd: RestoreSnapshot) =>
      onRestoreComponent(cmd)
  }

  private def onInitializeComponent(cmd: Init): Future[Done] = {
    tryInitializeComponent().map(_ => Done)
  }

  private def onSnapshotComponent(cmd: CreateSnapshot): Future[Done] = {

    restorableComponent match {
      case cp: Snapshottable =>
        cp.snapshotState(cmd.snapshotId, cmd.snapshotTime).flatMap { state =>
          stateManager.saveComponentState(state,
                                          componentId,
                                          cmd.snapshotId,
                                          cmd.snapshotTime,
                                          cmd.collector)
        }
      case w: DataWriter[_] =>
        // writers have to report some state
        stateManager.saveComponentState(EmptyState,
                                        componentId,
                                        cmd.snapshotId,
                                        cmd.snapshotTime,
                                        cmd.collector)
      case _ =>
        Future.successful(Done)
    }
  }

  private def onRestoreComponent(cmd: RestoreSnapshot): Future[Done] = {

    val restore = restorableComponent match {
      case cp: Snapshottable =>
        stateManager
          .loadComponentState[Serializable](componentId, cmd.snapshotId)
          .flatMap { state =>
            cp.restoreState(state.asInstanceOf[cp.Snapshot]).map { _ =>
              Done
            }
          }
      case _ =>
        Future.successful(Done)
    }

    restore.flatMap { _ =>
      tryInitializeComponent().map(_ => Done)
    }
  }
}
