/*
 * Copyright 2018 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka.consumer

import fs2.kafka.AcknowledgeableConsumerRecord
import fs2.Stream

trait KafkaShareConsume[F[_], K, V] {

  /**
    * Alias for [[stream]].
    */
  final def records: Stream[F, AcknowledgeableConsumerRecord[F, K, V]] = stream

  /**
    * `Stream` of the records received by the share consumer, in the order they were polled. The
    * consumer polls again once every record of the previous poll has a decision, or in implicit
    * mode once the stream took the previous poll. Java's share consumer returns renewed records
    * from a later poll again, but they are not emitted again here, and stay valid for
    * acknowledgement. A poll is taken as one chunk. When the stream ends, the records of a poll it
    * never took are released; records skipped by operators such as `take` stay held until the
    * consumer closes.
    *
    * Note that in implicit acknowledgement mode, the records of a poll are accepted by the next
    * poll, which happens once the stream took them, even if they are still being processed. A
    * record whose processing then fails is not delivered again (at-most-once). Use explicit
    * acknowledgement, the default, for at-least-once delivery.
    *
    * The stream can be started only once, or it fails with a
    * [[fs2.kafka.StreamAlreadyStartedException]]. To consume again after the stream has ended,
    * create a new share consumer, which does not disturb the other consumers of the share group.
    * Subscribe before starting it, or it fails with a [[fs2.kafka.NotSubscribedException]].
    */
  def stream: Stream[F, AcknowledgeableConsumerRecord[F, K, V]]

  /**
    * Stops consuming new messages from Kafka. In explicit acknowledgement mode, polled records can
    * still be acknowledged, and the stream ends once all of them are, after a final commit of the
    * staged decisions. In implicit mode, the stream ends once it took the latest poll, which the
    * final commit accepts.
    */
  def stopConsuming: F[Unit]

}
