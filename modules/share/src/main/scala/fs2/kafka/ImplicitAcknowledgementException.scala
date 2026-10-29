/*
 * Copyright 2018 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka

import org.apache.kafka.common.KafkaException

/**
  * [[ImplicitAcknowledgementException]] indicates an acknowledgement on a share consumer in
  * implicit acknowledgement mode, where the records of a poll are accepted by the next poll or
  * commit instead, as with Java's share consumer.
  */
sealed abstract class ImplicitAcknowledgementException
    extends KafkaException(
      "records cannot be acknowledged with share.acknowledgement.mode=implicit; they are accepted by the next poll"
    )

private[kafka] object ImplicitAcknowledgementException {

  def apply(): ImplicitAcknowledgementException =
    new ImplicitAcknowledgementException {

      override def toString: String =
        s"fs2.kafka.ImplicitAcknowledgementException: $getMessage"

    }

}
