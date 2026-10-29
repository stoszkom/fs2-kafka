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

import org.apache.kafka.common.TopicPartition

/**
  * An offset and metadata for a topic and partition which can be acknowledged.
  *
  * [[acknowledge]] acknowledges the record with an [[AcknowledgeType]], and returns an
  * [[AcknowledgedOffset]]. Nothing is sent to the broker until the [[AcknowledgedOffset]] is
  * committed.
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

  /**
    * The committer used to commit the [[AcknowledgeType]] and offset.
    */
  def committer: KafkaShareCommitter[F]

  /**
    * Creates an AcknowledgedOffset with the given [[AcknowledgeType]]. Nothing is sent to the
    * broker until the returned [[AcknowledgedOffset]] is committed.
    */
  final def acknowledge(acknowledgeType: AcknowledgeType): AcknowledgedOffset[F] =
    AcknowledgedOffset(topicPartition, offset, deliveryCount, acknowledgeType, committer)

  /**
    * Creates a new [[AcknowledgeableOffset]] in which the effect type has been changed using the
    * specified `FunctionK`.
    */
  final def mapK[G[_]](f: F ~> G): AcknowledgeableOffset[G] =
    AcknowledgeableOffset(topicPartition, offset, deliveryCount, committer.mapK(f))

}

object AcknowledgeableOffset {

  final private[this] case class AcknowledgeableOffsetImpl[F[_]](
    override val topicPartition: TopicPartition,
    override val offset: Long,
    override val deliveryCount: Short,
    override val committer: KafkaShareCommitter[F]
  ) extends AcknowledgeableOffset[F] {

    override def toString: String =
      s"AcknowledgeableOffset(${topicPartition.show} -> $offset, deliveryCount = ${deliveryCount
          .toInt}, $committer)"

  }

  private[kafka] def apply[F[_]](
    topicPartition: TopicPartition,
    offset: Long,
    deliveryCount: Short,
    committer: KafkaShareCommitter[F]
  ): AcknowledgeableOffset[F] =
    AcknowledgeableOffsetImpl(topicPartition, offset, deliveryCount, committer)

  implicit def acknowledgeableOffsetEq[F[_]]: Eq[AcknowledgeableOffset[F]] =
    Eq.fromUniversalEquals

  implicit def acknowledgeableOffsetShow[F[_]]: Show[AcknowledgeableOffset[F]] =
    Show.fromToString

}
