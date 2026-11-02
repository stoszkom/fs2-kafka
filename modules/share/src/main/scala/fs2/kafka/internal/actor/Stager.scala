/*
 * Copyright 2018 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka.internal.actor

import cats.~>
import fs2.kafka.AcknowledgeType
import fs2.Chunk

// A decision for one delivery of a record.
final private[kafka] case class Decision(
  offset: TopicPartitionOffset,
  deliveryCount: Short,
  acknowledgeType: AcknowledgeType
)

// Stages decisions in the Java consumer of the records they are for. `acknowledgeChunks` groups
// offsets by stager: a consumer's actor by reference, a mapped one by its stager and function.
private[kafka] trait Stager[F[_]] {

  def stage(decisions: Chunk[Decision]): F[Unit]

}

private[kafka] object Stager {

  final private case class Mapped[F[_], G[_]](underlying: Stager[F], f: F ~> G) extends Stager[G] {

    override def stage(decisions: Chunk[Decision]): G[Unit] = f(underlying.stage(decisions))

  }

  def mapK[F[_], G[_]](stager: Stager[F], f: F ~> G): Stager[G] = Mapped(stager, f)

}
