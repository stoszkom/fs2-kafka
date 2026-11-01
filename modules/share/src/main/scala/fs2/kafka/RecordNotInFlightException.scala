/*
 * Copyright 2018 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka

import org.apache.kafka.common.KafkaException
import org.apache.kafka.common.TopicPartition

/**
  * [[RecordNotInFlightException]] indicates that an acknowledgement was for a delivery which the
  * share consumer no longer holds: it was already acknowledged, its lock renewal failed, the
  * consumer released it after its max processing time passed, or the acknowledgement belongs to an
  * earlier delivery of the same record. Unless it was already decided, the record is delivered
  * again, so the decision can usually be dropped.
  */
sealed abstract class RecordNotInFlightException(topicPartition: TopicPartition, offset: Long)
    extends KafkaException(
      s"record at offset $offset of $topicPartition is not in flight on this share consumer"
    )

private[kafka] object RecordNotInFlightException {

  def apply(topicPartition: TopicPartition, offset: Long): RecordNotInFlightException =
    new RecordNotInFlightException(topicPartition, offset) {

      override def toString: String =
        s"fs2.kafka.RecordNotInFlightException: $getMessage"

    }

}
