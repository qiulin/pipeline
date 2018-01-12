package com.commodityvectors.pipeline

import scala.concurrent._
import scala.language.postfixOps

import akka.Done
import com.commodityvectors.pipeline.state.MessageComponentStateManager
import com.commodityvectors.pipeline.util.FutureQueue

private class MessageReader[A](reader: DataReader[A], id: String)(
    stateManager: MessageComponentStateManager)
    extends MessageComponent(reader, id)(stateManager) {

  import MessageReader._
  import util.ExecutionContexts.Implicits.sameThreadExecutionContext

  override def componentType = MessageComponentType.Reader

  private lazy val notificationsQueue: FutureQueue[MessageNotification] =
    new FutureQueue[MessageNotification]()

  private var stopped: Boolean = false
  private var userMessageFetching: Boolean = false

  def close(): Unit = {
    reader.close()
  }

  def readMessage(): Future[Option[Message[A]]] = {

    // when component is ready
    initialized.flatMap { _ =>
      nextMessage().flatMap {
        case opt @ Some(msg) =>
          processMessage(msg).map(_ => opt)
        case _ =>
          Future.successful(None)
      }
    }
  }

  /**
    * Return either user or system message notification - whichever arrives first,
    * but keep other future cached for next call.
    */
  private def nextMessageNotification(): Future[MessageNotification] = {
    if (notificationsQueue.size < MaxPrefetch && !userMessageFetching && isComponentInitialized && !stopped) {
      userMessageFetching = true
      reader
        .fetch()
        .map { n =>
          userMessageFetching = false
          if (n > 0) {
            // enqueue notification for each available user message
            for (_ <- 1 to n) {
              notificationsQueue.enqueue(UserMessageNotification)
            }
          } else {
            // this means EOF
            notificationsQueue.enqueue(EofNotification)
          }
        }
        .recover {
          case e =>
            userMessageFetching = false
            notificationsQueue.enqueue(ErrorNotification(e))
        }
    }

    notificationsQueue.dequeue()
  }

  private def nextMessage(): Future[Option[Message[A]]] = {
    if (stopped) {
      Future.successful(None)
    } else {
      nextMessageNotification().map {
        case ErrorNotification(t) =>
          stopped = true
          throw t
        case EofNotification =>
          stopped = true
          None
        case InjectedMessageNotification(msg: Message[A]) =>
          Some(msg)
        case UserMessageNotification =>
          reader.pull() match {
            case Some(data) =>
              Some(Message.user(data))
            case _ =>
              throw new Exception(
                s"Contract violation by $reader: pull didn't return any data, but it should have according to fetch result.")
          }
      }
    }
  }

  private def processMessage(msg: Message[A]): Future[Done] = msg match {
    case msg @ Message.system(_, _) =>
      onSystemMessage(msg)
    case _ =>
      Future.successful(Done)
  }

  private[pipeline] def injectMessage(message: Message[A]): Unit = {
    notificationsQueue.enqueue(InjectedMessageNotification(message))
  }
}

object MessageReader {

  private val MaxPrefetch = 4

  private sealed trait MessageNotification

  private case class ErrorNotification(t: Throwable) extends MessageNotification

  private case object EofNotification extends MessageNotification

  private case object UserMessageNotification extends MessageNotification

  private case class InjectedMessageNotification[A](msg: Message[A])
      extends MessageNotification

}
