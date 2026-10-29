/*
 * Copyright 2018 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka.internal.actor

import fs2.kafka.AcknowledgeType
import fs2.Chunk

import org.apache.kafka.common.{KafkaException, TopicPartition}

sealed abstract private[kafka] class ShareRequest[F[_]]

private[kafka] object ShareRequest {

  final case class Start[F[_]]() extends ShareRequest[F]

  final case class Stop[F[_]]() extends ShareRequest[F]

  final case class HandedOut[F[_]]() extends ShareRequest[F]

  final case class ReaderClosed[F[_]]() extends ShareRequest[F]

  final case class Commit[F[_]](
    acknowledgements: Chunk[Ack],
    sync: Boolean,
    callback: Either[Throwable, Map[TopicPartition, Option[KafkaException]]] => F[Unit]
  ) extends ShareRequest[F]

  final case class Ack(
    topicPartition: TopicPartition,
    offset: Long,
    deliveryCount: Option[Short],
    acknowledgeType: AcknowledgeType
  ) {
    def id: ShareState.Id = (topicPartition, offset)
  }

}
