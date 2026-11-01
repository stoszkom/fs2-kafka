/*
 * Copyright 2018 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka.internal.actor

import cats.~>
import fs2.kafka.AcknowledgeType
import fs2.Chunk

import org.apache.kafka.common.TopicPartition

// A decision for one delivery of a record.
final private[kafka] case class Decision(
  topicPartition: TopicPartition,
  offset: Long,
  deliveryCount: Short,
  acknowledgeType: AcknowledgeType
) {

  def position: TopicPartitionOffset = topicPartition -> offset

}

// Stages decisions in the Java consumer of the records they are for.
private[kafka] trait Stager[F[_]] {

  def stage(decisions: Chunk[Decision]): F[Unit]

  // The consumer the decisions go to, so that `acknowledgeChunks` groups offsets by consumer, also
  // after `mapK`.
  def consumer: AnyRef = this

}

private[kafka] object Stager {

  final private case class Mapped[F[_], G[_]](underlying: Stager[F], f: F ~> G) extends Stager[G] {

    override def stage(decisions: Chunk[Decision]): G[Unit] = f(underlying.stage(decisions))

    override def consumer: AnyRef = underlying.consumer

  }

  def mapK[F[_], G[_]](stager: Stager[F], f: F ~> G): Stager[G] = Mapped(stager, f)

}
