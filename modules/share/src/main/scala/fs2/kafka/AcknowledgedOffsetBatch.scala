/*
 * Copyright 2018 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka

import cats.syntax.all.*
import cats.Eq
import cats.Foldable
import cats.Parallel
import cats.Show
import fs2.kafka.internal.actor.ShareCommits
import fs2.Chunk

import org.apache.kafka.common.{KafkaException, TopicPartition}

/**
  * Offsets and acknowledge types for topics and partitions which can be committed, similar to
  * [[CommittableOffsetBatch]].
  *
  * Note they are grouped by the consumer ([[KafkaShareCommitter]] used here) as they need to be
  * committed by the same share consumer that polled them (support for the concurrent share
  * consumers pattern)
  */
sealed abstract class AcknowledgedOffsetBatch[F[_]] {

  /**
    * Returns a new batch with the specified offset included.
    */
  def updated(that: AcknowledgedOffset[F]): AcknowledgedOffsetBatch[F]

  /**
    * Returns a new batch with the offsets of the specified batch included.
    */
  def updated(that: AcknowledgedOffsetBatch[F]): AcknowledgedOffsetBatch[F]

  /**
    * Returns the offsets included in the batch, by the committer of their consumer.
    */
  def offsets: Map[KafkaShareCommitter[F], Chunk[AcknowledgedOffset[F]]]

  /**
    * Commits the offsets and acknowledge types for the topics and partitions.
    *
    * Note done per consumer that polled the records.
    */
  def commit: F[Unit]

  /**
    * Like [[commit]], but returns the result per partition instead of failing: `None` if committed,
    * otherwise the error, which applies to all the partition's offsets (the first one if several
    * consumers committed them). Still fails if a whole commit fails, for example with a
    * [[ShareCommitTimeoutException]].
    */
  def commitWithResults: F[Map[TopicPartition, Option[KafkaException]]]

}

object AcknowledgedOffsetBatch {

  final private[this] case class AcknowledgedOffsetBatchImpl[F[_]: Parallel](
    override val offsets: Map[KafkaShareCommitter[F], Chunk[AcknowledgedOffset[F]]]
  ) extends AcknowledgedOffsetBatch[F] {

    override def updated(that: AcknowledgedOffset[F]): AcknowledgedOffsetBatch[F] =
      copy(
        offsets.updated(
          that.committer,
          offsets.getOrElse(that.committer, Chunk.empty) ++ Chunk.singleton(that)
        )
      )

    override def updated(that: AcknowledgedOffsetBatch[F]): AcknowledgedOffsetBatch[F] =
      copy(
        that
          .offsets
          .foldLeft(offsets) { case (acc, (committer, values)) =>
            acc.updated(committer, acc.getOrElse(committer, Chunk.empty) ++ values)
          }
      )

    override def commit: F[Unit] =
      offsets
        .toList
        .parTraverse_ { case (committer, values) =>
          committer.commitAcknowledged(values)
        }

    override def commitWithResults: F[Map[TopicPartition, Option[KafkaException]]] =
      Parallel[F]
        .monad
        .map(
          offsets
            .toList
            .parTraverse { case (committer, values) =>
              committer.commitAcknowledgedWithResults(values)
            }
        )(
          _.foldLeft(Map.empty[TopicPartition, Option[KafkaException]])(
            ShareCommits.mergeResults
          )
        )

    override def toString: String =
      s"AcknowledgedOffsetBatch(${offsets.values.flatMap(_.toList).mkString(", ")})"

  }

  /**
    * Returns a new batch from the specified foldable offsets.
    */
  def fromFoldable[F[_]: Parallel, G[_]: Foldable](
    offsets: G[AcknowledgedOffset[F]]
  ): AcknowledgedOffsetBatch[F] =
    fromFoldableMap(offsets)(identity)

  /**
    * Returns a new batch from the specified foldable elements.
    *
    * The provided function is used on each element to retrieve the offsets.
    */
  def fromFoldableMap[F[_]: Parallel, G[_]: Foldable, A](
    ga: G[A]
  )(f: A => AcknowledgedOffset[F]): AcknowledgedOffsetBatch[F] =
    ga.foldLeft(empty[F]) { case (batch, a) => batch.updated(f(a)) }

  /**
    * Returns a new batch from the specified optional offsets.
    *
    * A `None` leaves its record out of the batch. In explicit acknowledgement mode, such records
    * still need to be acknowledged and committed elsewhere, or their consumer does not poll new
    * records.
    */
  def fromFoldableOption[F[_]: Parallel, G[_]: Foldable](
    offsets: G[Option[AcknowledgedOffset[F]]]
  ): AcknowledgedOffsetBatch[F] =
    offsets.foldLeft(empty[F]) {
      case (batch, Some(offset)) => batch.updated(offset)
      case (batch, None)         => batch
    }

  /**
    * Returns an empty batch.
    */
  def empty[F[_]: Parallel]: AcknowledgedOffsetBatch[F] =
    AcknowledgedOffsetBatchImpl(Map.empty)

  implicit def acknowledgedOffsetBatchEq[F[_]]: Eq[AcknowledgedOffsetBatch[F]] =
    Eq.fromUniversalEquals

  implicit def acknowledgedOffsetBatchShow[F[_]]: Show[AcknowledgedOffsetBatch[F]] =
    Show.fromToString

}
