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
  * The offset of a record held by a [[KafkaShareConsumer]], with the delivery it belongs to.
  */
sealed abstract class AcknowledgeableOffset[F[_]] { self =>

  /**
    * The topic and partition of the record.
    */
  def topicPartition: TopicPartition

  /**
    * The offset for the topic and partition.
    */
  def offset: Long

  /**
    * How many times the record has been delivered, including this delivery.
    */
  def deliveryCount: Short

  private[kafka] def stager: Stager[F]

  /**
    * Stages the decision in the Java consumer, as Java's `acknowledge` does. A later decision
    * replaces it until it is sent, by the next poll, a commit, or after the
    * [[ShareConsumerSettings.maxAcknowledgementDelay]]. Once sent, or for a delivery the consumer
    * no longer holds, a decision fails with a [[RecordNotInFlightException]]. A decision for a
    * record being renewed completes once a poll returns the record and Java staged the decision, or
    * fails if the record loses its lock first. In implicit acknowledgement mode, it fails with an
    * [[ImplicitAcknowledgementException]].
    */
  final def acknowledge(acknowledgeType: AcknowledgeType): F[Unit] =
    stager.stage(Chunk.singleton(decision(acknowledgeType)))

  /**
    * Stages an `Accept`: the record was processed. See [[acknowledge]].
    */
  final def accept: F[Unit] = acknowledge(AcknowledgeType.Accept)

  /**
    * Stages a `Release`: the record is delivered again. See [[acknowledge]].
    */
  final def release: F[Unit] = acknowledge(AcknowledgeType.Release)

  /**
    * Stages a `Reject`: the record is not delivered again. See [[acknowledge]].
    */
  final def reject: F[Unit] = acknowledge(AcknowledgeType.Reject)

  /**
    * Renews the record's acquisition lock; it needs another decision later. See [[acknowledge]].
    */
  final def renew: F[Unit] = acknowledge(AcknowledgeType.Renew)

  private[kafka] def decision(acknowledgeType: AcknowledgeType): Decision =
    Decision(topicPartition -> offset, deliveryCount, acknowledgeType)

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
