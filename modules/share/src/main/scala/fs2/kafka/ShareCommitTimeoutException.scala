/*
 * Copyright 2018 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka

import scala.concurrent.duration.FiniteDuration

import org.apache.kafka.common.errors.TimeoutException
import org.apache.kafka.common.TopicPartition

/**
  * [[ShareCommitTimeoutException]] indicates that the broker did not confirm committed
  * acknowledgements within the share consumer's commit timeout, like [[CommitTimeoutException]] for
  * `KafkaConsumer`. The acknowledgements may still have been applied. The timeout and the offsets,
  * with their acknowledge types, are included in the exception message.
  */
sealed abstract class ShareCommitTimeoutException(
  timeout: FiniteDuration,
  offsets: Map[TopicPartition, Map[Long, AcknowledgeType]]
) extends TimeoutException(
      offsets
        .toList
        .sortBy { case (partition, _) => (partition.topic, partition.partition) }
        .map { case (partition, types) =>
          types
            .toList
            .sortBy(_._1)
            .map { case (offset, acknowledgeType) => s"$offset: $acknowledgeType" }
            .mkString(s"$partition -> {", ", ", "}")
        }
        .mkString(s"share commit timeout after $timeout for offsets: ", ", ", "")
    )

private[kafka] object ShareCommitTimeoutException {

  def apply(
    timeout: FiniteDuration,
    offsets: Map[TopicPartition, Map[Long, AcknowledgeType]]
  ): ShareCommitTimeoutException =
    new ShareCommitTimeoutException(timeout, offsets) {

      override def toString: String =
        s"fs2.kafka.ShareCommitTimeoutException: $getMessage"

    }

}
