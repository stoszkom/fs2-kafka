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
    * share consumer emits only records from its latest poll. Java's share consumer returns renewed
    * records from the next poll again, but here they are tracked internally between the polls and
    * stay valid for acknowledgement.
    *
    * Note that in implicit acknowledgement mode, the records of a poll are accepted by the next
    * poll, which happens once all of them have been emitted here, even if they are still being
    * processed. A record whose processing then fails is not delivered again (at-most-once). Use
    * explicit acknowledgement, the default, for at-least-once delivery.
    *
    * The stream can be started only once, or it fails with a
    * [[fs2.kafka.StreamAlreadyStartedException]]. To consume again after the stream has ended,
    * create a new share consumer, which does not disturb the other consumers of the share group.
    * Subscribe before starting it, or it fails with a [[fs2.kafka.NotSubscribedException]].
    */
  def stream: Stream[F, AcknowledgeableConsumerRecord[F, K, V]]

  /**
    * Stops consuming new messages from Kafka. In explicit acknowledgement mode, polled records can
    * still be acknowledged, and the stream ends once all of them are. In implicit mode, the stream
    * ends once the polled records are emitted.
    */
  def stopConsuming: F[Unit]

}
