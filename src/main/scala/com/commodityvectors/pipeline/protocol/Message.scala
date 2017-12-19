package com.commodityvectors.pipeline.protocol

import com.commodityvectors.pipeline.protocol

/**
  * Low level message envelope.
  * Can be either: user message or system command.
  *
  * Biased towards user message.
  *
  * @tparam A user message type
  */
sealed trait Message[+A] {

  def headers: MessageHeaders

  def data: Option[A]

  def flatMap[B](f: A => Message[B]): Message[B]

  def map[B](f: A => B): Message[B]

  def isSystem: Boolean

  def isUser: Boolean = !isSystem

  def copy(headers: MessageHeaders): Message[A]
}

object Message {

  private def defaultHeaders = MessageHeaders(0)

  def user[A](body: A): UserMessage[A] =
    protocol.UserMessage(defaultHeaders, body)

  def user[A](headers: MessageHeaders, body: A): UserMessage[A] =
    protocol.UserMessage(headers, body)

  def system[A <: Command](body: A): SystemMessage[A] =
    protocol.SystemMessage(defaultHeaders, body)

  def system[A <: Command](headers: MessageHeaders, body: A): SystemMessage[A] =
    protocol.SystemMessage(headers, body)

  object user {
    def unapply[A](msg: UserMessage[A]): Option[(MessageHeaders, A)] =
      protocol.UserMessage.unapply(msg)
  }

  object system {
    def unapply[A <: Command](
        msg: SystemMessage[A]): Option[(MessageHeaders, A)] =
      protocol.SystemMessage.unapply(msg)
  }
}

/**
  * User message
  * @param headers message headers
  * @param body user data
  * @tparam A user data type
  */
case class UserMessage[A](headers: MessageHeaders, body: A) extends Message[A] {

  override def isSystem: Boolean = false

  override def flatMap[B](f: (A) => Message[B]): Message[B] = f(body)

  override def map[B](f: (A) => B): Message[B] = copy(body = f(body))

  override def copy(headers: MessageHeaders): Message[A] =
    protocol.UserMessage(headers, body)

  def copy[B](headers: MessageHeaders = headers,
              body: B = body): UserMessage[B] =
    protocol.UserMessage(headers, body)

  override def data: Option[A] = Some(body)
}

/**
  * System message
  * @param headers message headers
  * @param body system command
  * @tparam C command type
  */
case class SystemMessage[C <: Command](headers: MessageHeaders, body: C)
    extends Message[Nothing] {

  override def isSystem: Boolean = true

  override def flatMap[B](f: (Nothing) => Message[B]): Message[B] = this

  override def map[B](f: (Nothing) => B): Message[B] = this

  override def copy(headers: MessageHeaders): Message[Nothing] =
    protocol.SystemMessage(headers, body)

  def copy[B <: Command](headers: MessageHeaders = headers,
                         body: B = body): SystemMessage[B] =
    protocol.SystemMessage(headers, body)

  override def data: Option[Nothing] = None
}
