/*
 * Copyright 2018 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka

import cats.~>
import cats.Eq
import cats.Show
import fs2.Chunk

import org.apache.kafka.common.{KafkaException, TopicPartition}

/**
  * Describes commit-related capabilities for a particular share consumer instance.
  */
sealed abstract class KafkaShareCommitter[F[_]] { self =>

  /**
    * Commits the offsets and acknowledgeTypes for the topic and partition. Can fail with:
    * [[RecordNotInFlightException]] when the consumer no longer holds the delivery,
    * [[ShareCommitTimeoutException]] when the broker does not confirm it within the commit timeout,
    * [[ImplicitAcknowledgementException]] in implicit acknowledgement mode,
    * [[ConsumerShutdownException]] when the consumer has been closed. Other errors reported by the
    * broker are raised as they are.
    */
  def commit(offsets: Map[TopicPartition, Map[Long, AcknowledgeType]]): F[Unit]

  /**
    * Commits a batch of [[AcknowledgedOffset]]
    */
  private[kafka] def commitAcknowledged(offsets: Chunk[AcknowledgedOffset[F]]): F[Unit]

  /**
    * Like [[commitAcknowledged]], but completes with the result for each partition of the offsets:
    * `None` when the partition's offsets were committed, or the error for them. Fails only when the
    * whole commit fails.
    */
  private[kafka] def commitAcknowledgedWithResults(
    offsets: Chunk[AcknowledgedOffset[F]]
  ): F[Map[TopicPartition, Option[KafkaException]]]

  /**
    * The number of records of the share consumer's latest poll for which no acknowledgement was
    * requested yet.
    */
  private[kafka] def pending: F[Int]

  /**
    * Creates a new [[KafkaShareCommitter]] in which the effect type has been changed using the
    * specified `FunctionK`.
    *
    * The resulting committer is equal to this one, so offsets committed through either of them can
    * still be merged into a single commit by [[AcknowledgedOffsetBatch]].
    */
  final def mapK[G[_]](f: F ~> G): KafkaShareCommitter[G] =
    KafkaShareCommitter.create(
      Some(source),
      offsets => f(self.commit(offsets)),
      offsets => f(self.commitAcknowledged(offsets.map(a => a.withCommitter(self)))),
      offsets => f(self.commitAcknowledgedWithResults(offsets.map(a => a.withCommitter(self)))),
      f(self.pending)
    )

  /**
    * The committer this one was derived from through [[mapK]], or this committer itself. Committers
    * with the same source commit through the same consumer, so this is what equality is based on.
    */
  private[kafka] def source: AnyRef

  final override def equals(that: Any): Boolean =
    that match {
      case that: KafkaShareCommitter[_] => source eq that.source
      case _                            => false
    }

  final override def hashCode(): Int =
    System.identityHashCode(source)

  final override def toString: String =
    "KafkaShareCommitter$" + System.identityHashCode(source)

}

object KafkaShareCommitter {

  private[kafka] def apply[F[_]](
    commitOffsets: Map[TopicPartition, Map[Long, AcknowledgeType]] => F[Unit],
    commitDeliveries: Chunk[AcknowledgedOffset[F]] => F[Unit],
    commitDeliveriesWithResults: Chunk[AcknowledgedOffset[F]] => F[
      Map[TopicPartition, Option[KafkaException]]
    ],
    pendingRecords: F[Int]
  ): KafkaShareCommitter[F] =
    create(None, commitOffsets, commitDeliveries, commitDeliveriesWithResults, pendingRecords)

  // Without a source, the new committer is its own source.
  private def create[F[_]](
    committerSource: Option[AnyRef],
    commitOffsets: Map[TopicPartition, Map[Long, AcknowledgeType]] => F[Unit],
    commitDeliveries: Chunk[AcknowledgedOffset[F]] => F[Unit],
    commitDeliveriesWithResults: Chunk[AcknowledgedOffset[F]] => F[
      Map[TopicPartition, Option[KafkaException]]
    ],
    pendingRecords: F[Int]
  ): KafkaShareCommitter[F] =
    new KafkaShareCommitter[F] {
      override def commit(
        offsets: Map[TopicPartition, Map[Long, AcknowledgeType]]
      ): F[Unit] = commitOffsets(offsets)

      override private[kafka] def commitAcknowledged(
        offsets: Chunk[AcknowledgedOffset[F]]
      ): F[Unit] = commitDeliveries(offsets)

      override private[kafka] def commitAcknowledgedWithResults(
        offsets: Chunk[AcknowledgedOffset[F]]
      ): F[Map[TopicPartition, Option[KafkaException]]] = commitDeliveriesWithResults(offsets)

      override private[kafka] val pending: F[Int] = pendingRecords

      override private[kafka] val source: AnyRef = committerSource.getOrElse(this)
    }

  implicit def kafkaShareCommitterEq[F[_]]: Eq[KafkaShareCommitter[F]] =
    Eq.fromUniversalEquals

  implicit def kafkaShareCommitterShow[F[_]]: Show[KafkaShareCommitter[F]] =
    Show.fromToString

}
