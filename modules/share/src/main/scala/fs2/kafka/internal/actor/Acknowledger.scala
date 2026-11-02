/*
 * Copyright 2018 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka.internal.actor

import cats.~>
import fs2.kafka.AcknowledgeType
import fs2.Chunk

// An acknowledgement for one delivery of a record.
final private[kafka] case class Acknowledgement(
  offset: TopicPartitionOffset,
  deliveryCount: Short,
  acknowledgeType: AcknowledgeType
)

// Gives acknowledgements to the Java consumer of the records they are for. `acknowledgeChunks` groups
// offsets by acknowledger: a consumer's actor by reference, a mapped one by its acknowledger and
// function.
private[kafka] trait Acknowledger[F[_]] {

  def acknowledge(acks: Chunk[Acknowledgement]): F[Unit]

}

private[kafka] object Acknowledger {

  final private case class Mapped[F[_], G[_]](underlying: Acknowledger[F], f: F ~> G)
      extends Acknowledger[G] {

    override def acknowledge(acks: Chunk[Acknowledgement]): G[Unit] = f(
      underlying.acknowledge(acks)
    )

  }

  def mapK[F[_], G[_]](acknowledger: Acknowledger[F], f: F ~> G): Acknowledger[G] =
    Mapped(acknowledger, f)

}
