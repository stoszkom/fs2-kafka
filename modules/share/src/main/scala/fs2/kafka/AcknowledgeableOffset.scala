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
import fs2.kafka.internal.actor.{Decision, Stager}
import fs2.Chunk

import org.apache.kafka.common.TopicPartition

/**
  * The position of a record held by a [[KafkaShareConsumer]], with the delivery it belongs to.
  */
sealed abstract class AcknowledgeableOffset[F[_]] { self =>

  /**
    * The topic and partition for the offset and metadata.
    */
  def topicPartition: TopicPartition

  /**
    * The offset for the topic and partition. Note there is no metadata for share consumer records
    */
  def offset: Long

  /**
    * How many times the record has been delivered, including this delivery.
    */
  def deliveryCount: Short

  private[kafka] def stager: Stager[F]

  /**
    * Stages the decision in the Java consumer, as Java's `acknowledge` does. A later decision for
    * the record replaces it until the consumer sends it: with its next poll, with
    * [[KafkaShareConsumer.commit]], or with a renewal or release it sends itself. The consumer
    * polls as soon as every record of its latest poll has a decision, so a decision made after that
    * fails with a [[RecordNotInFlightException]], as does one for a delivery the consumer no longer
    * holds. A decision for a record being renewed waits until a poll returns it, and is dropped if
    * the renewal fails first, as the record is delivered again. In implicit acknowledgement mode,
    * it fails with an [[ImplicitAcknowledgementException]].
    */
  final def acknowledge(acknowledgeType: AcknowledgeType): F[Unit] =
    stager.stage(Chunk.singleton(decision(acknowledgeType)))

  private[kafka] def decision(acknowledgeType: AcknowledgeType): Decision =
    Decision(topicPartition, offset, deliveryCount, acknowledgeType)

  /**
    * Creates a new [[AcknowledgeableOffset]] in which the effect type has been changed using the
    * specified `FunctionK`.
    */
  final def mapK[G[_]](f: F ~> G): AcknowledgeableOffset[G] =
    AcknowledgeableOffset(topicPartition, offset, deliveryCount, Stager.mapK(stager, f))

}

object AcknowledgeableOffset {

  final private[this] case class AcknowledgeableOffsetImpl[F[_]](
    override val topicPartition: TopicPartition,
    override val offset: Long,
    override val deliveryCount: Short,
    override private[kafka] val stager: Stager[F]
  ) extends AcknowledgeableOffset[F] {

    override def toString: String =
      s"AcknowledgeableOffset(${topicPartition.show} -> $offset, deliveryCount = ${deliveryCount
          .toInt})"

  }

  private[kafka] def apply[F[_]](
    topicPartition: TopicPartition,
    offset: Long,
    deliveryCount: Short,
    stager: Stager[F]
  ): AcknowledgeableOffset[F] =
    AcknowledgeableOffsetImpl(topicPartition, offset, deliveryCount, stager)

  implicit def acknowledgeableOffsetEq[F[_]]: Eq[AcknowledgeableOffset[F]] =
    Eq.fromUniversalEquals

  implicit def acknowledgeableOffsetShow[F[_]]: Show[AcknowledgeableOffset[F]] =
    Show.fromToString

}
