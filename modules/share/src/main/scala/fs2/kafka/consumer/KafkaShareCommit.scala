/*
 * Copyright 2018 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka.consumer

import fs2.kafka.AcknowledgeType

import org.apache.kafka.common.TopicPartition

trait KafkaShareCommit[F[_]] {

  /**
    * Commit the specified acknowledged offsets for the specified list of topics and partitions to
    * Kafka.<br><br>
    *
    * Unlike [[KafkaCommit.commitAsync]], this does not commit a position for the group. Each offset
    * is a single record held by this consumer, and its [[AcknowledgeType]] decides what happens to
    * it: `Accept` completes it, `Release` delivers it again, `Reject` archives it and `Renew`
    * extends its acquisition lock. Acknowledging an offset says nothing about the other offsets
    * from the same partition.<br><br>
    *
    * Each record must still be held by this consumer: returned by its poll and not yet
    * acknowledged, except after a `Renew`. Otherwise, its partition fails with a
    * [[RecordNotInFlightException]]. A record whose acquisition lock has expired fails with the
    * broker's `InvalidRecordStateException` for its partition. The acknowledgement mode
    * (`share.acknowledgement.mode`) must be explicit, otherwise it fails with
    * [[ImplicitAcknowledgementException]]. It fails with [[ShareCommitTimeoutException]] when Kafka
    * does not confirm the acknowledgements within the commit timeout.<br><br>
    *
    * This completes once Kafka has confirmed the acknowledgements, and fails with the first error
    * reported for any partition.<br><br>
    *
    * Acknowledgements of the same record are guaranteed to be sent in the same order as the
    * invocations.<br><br>
    *
    * Note, that the recommended way for committing offsets in fs2-kafka is to use `commit` on
    * [[AcknowledgedOffset]] or [[AcknowledgedOffsetBatch]], or `commitWithResults` on
    * [[AcknowledgedOffsetBatch]] to get the result for each partition. [[commitAsync]] and
    * [[commitSync]] are usually needed only for some custom scenarios.
    *
    * @param offsets
    *   A map of acknowledge types by offset, by partition.
    * @see
    *   org.apache.kafka.clients.consumer.ShareConsumer#acknowledge
    * @see
    *   org.apache.kafka.clients.consumer.ShareConsumer#commitAsync
    */
  def commitAsync(offsets: Map[TopicPartition, Map[Long, AcknowledgeType]]): F[Unit]

  /**
    * Commit the specified acknowledged offsets for the specified list of topics and partitions to
    * Kafka.<br><br>
    *
    * Unlike [[KafkaCommit.commitSync]], this does not commit a position for the group. Each offset
    * is a single record held by this consumer, and its [[AcknowledgeType]] decides what happens to
    * it: `Accept` completes it, `Release` delivers it again, `Reject` archives it and `Renew`
    * extends its acquisition lock. Acknowledging an offset says nothing about the other offsets
    * from the same partition.<br><br>
    *
    * Each record must still be held by this consumer: returned by its poll and not yet
    * acknowledged, except after a `Renew`. Otherwise, its partition fails with a
    * [[RecordNotInFlightException]]. A record whose acquisition lock has expired fails with the
    * broker's `InvalidRecordStateException` for its partition. The acknowledgement mode
    * (`share.acknowledgement.mode`) must be explicit, otherwise it fails with
    * [[ImplicitAcknowledgementException]]. It fails with [[ShareCommitTimeoutException]] when Kafka
    * does not confirm the acknowledgements within the commit timeout.<br><br>
    *
    * This completes once Kafka has confirmed the acknowledgements, and fails with the first error
    * reported for any partition.<br><br>
    *
    * Despite its name, this method is not blocking. But it's based on the blocking
    * org.apache.kafka.clients.consumer.ShareConsumer#commitSync method.<br><br>
    *
    * Acknowledgements of the same record are guaranteed to be sent in the same order as the
    * invocations.<br><br>
    *
    * Note, that the recommended way for committing offsets in fs2-kafka is to use `commit` on
    * [[AcknowledgedOffset]] or [[AcknowledgedOffsetBatch]], or `commitWithResults` on
    * [[AcknowledgedOffsetBatch]] to get the result for each partition. [[commitAsync]] and
    * [[commitSync]] are usually needed only for some custom scenarios.
    *
    * @param offsets
    *   A map of acknowledge types by offset, by partition.
    * @see
    *   org.apache.kafka.clients.consumer.ShareConsumer#acknowledge
    * @see
    *   org.apache.kafka.clients.consumer.ShareConsumer#commitSync
    */
  def commitSync(offsets: Map[TopicPartition, Map[Long, AcknowledgeType]]): F[Unit]

}
