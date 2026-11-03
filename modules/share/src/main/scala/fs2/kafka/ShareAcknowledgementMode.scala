/*
 * Copyright 2018 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka

/**
  * The share consumer's acknowledgement mode, the `share.acknowledgement.mode` property, set with
  * [[ShareConsumerSettings.withAcknowledgementMode]].
  */
sealed abstract class ShareAcknowledgementMode

object ShareAcknowledgementMode {

  private[kafka] case object ExplicitAcknowledgement extends ShareAcknowledgementMode {
    override def toString: String = "Explicit"
  }

  private[kafka] case object ImplicitAcknowledgement extends ShareAcknowledgementMode {
    override def toString: String = "Implicit"
  }

  /**
    * Every record needs an acknowledgement before the next poll. The default here.
    */
  val Explicit: ShareAcknowledgementMode = ExplicitAcknowledgement

  /**
    * Every record of a poll is accepted once the stream has taken the poll, when the consumer next
    * sends acknowledgements: with the next poll, at once while paused, or when it stops. This makes
    * the stream at-most-once. Java's default.
    */
  val Implicit: ShareAcknowledgementMode = ImplicitAcknowledgement

}
