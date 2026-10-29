/*
 * Copyright 2018 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka

import cats.~>
import cats.syntax.all.*
import cats.Eq
import cats.Show
import fs2.kafka.instances.*
import fs2.Chunk

import org.apache.kafka.common.TopicPartition

/**
  * An offset and acknowledgeType for a topic and partition which can be committed.
  *
  * Note offsets from the same poll are most often committed in batches for performance reasons
  */
sealed abstract class AcknowledgedOffset[F[_]] { self =>

  /**
    * The topic and partition of the acknowledged record.
    */
  def topicPartition: TopicPartition

  /**
    * The offset of the acknowledged record in the topic and partition.
    */
  def offset: Long

  /**
    * How many times the record had been delivered when it is acknowledged, including the delivery
    * being acknowledged, so 1 for its first delivery.
    */
  def deliveryCount: Short

  /**
    * How the record is acknowledged: `Accept`, `Release`, `Reject` or `Renew`
    */
  def acknowledgeType: AcknowledgeType

  /**
    * The committer used to commit the offset and acknowledgeType.
    */
  def committer: KafkaShareCommitter[F]

  /**
    * Commits the offset and acknowledgeType for the topic and partition. Can fail with:
    * [[RecordNotInFlightException]] when the consumer no longer holds the delivery,
    * [[ShareCommitTimeoutException]] when the broker does not confirm it within the commit timeout,
    * [[ImplicitAcknowledgementException]] in implicit acknowledgement mode,
    * [[ConsumerShutdownException]] when the consumer has been closed. Other errors reported by the
    * broker are raised as they are.
    */
  final def commit: F[Unit] =
    committer.commitAcknowledged(Chunk.singleton(self))

  /**
    * Creates a new [[AcknowledgedOffset]] in which the effect type has been changed using the
    * specified `FunctionK`.
    */
  final def mapK[G[_]](f: F ~> G): AcknowledgedOffset[G] =
    withCommitter(committer.mapK(f))

  private[kafka] def withCommitter[G[_]](committer: KafkaShareCommitter[G]): AcknowledgedOffset[G] =
    AcknowledgedOffset(topicPartition, offset, deliveryCount, acknowledgeType, committer)

}

object AcknowledgedOffset {

  final private[this] case class AcknowledgedOffsetImpl[F[_]](
    override val topicPartition: TopicPartition,
    override val offset: Long,
    override val deliveryCount: Short,
    override val acknowledgeType: AcknowledgeType,
    override val committer: KafkaShareCommitter[F]
  ) extends AcknowledgedOffset[F] {

    override def toString: String =
      s"AcknowledgedOffset(${topicPartition.show} -> $offset, deliveryCount = ${deliveryCount
          .toInt}, $acknowledgeType, $committer)"

  }

  private[kafka] def apply[F[_]](
    topicPartition: TopicPartition,
    offset: Long,
    deliveryCount: Short,
    acknowledgeType: AcknowledgeType,
    committer: KafkaShareCommitter[F]
  ): AcknowledgedOffset[F] =
    AcknowledgedOffsetImpl(topicPartition, offset, deliveryCount, acknowledgeType, committer)

  implicit def acknowledgedOffsetEq[F[_]]: Eq[AcknowledgedOffset[F]] =
    Eq.fromUniversalEquals

  implicit def acknowledgedOffsetShow[F[_]]: Show[AcknowledgedOffset[F]] =
    Show.fromToString

}
