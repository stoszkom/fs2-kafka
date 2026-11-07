/*
 * Copyright 2018 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka.internal

import cats.~>
import fs2.Chunk
import fs2.kafka.AcknowledgeType
import fs2.kafka.internal.Acknowledgement.TopicPartitionOffset
import org.apache.kafka.common.TopicPartition

// An acknowledgement for one delivery of a record.
final private[kafka] case class Acknowledgement(
  offset: TopicPartitionOffset,
  deliveryCount: Short,
  acknowledgeType: AcknowledgeType
)

private[kafka] object Acknowledgement {

  // A record's offset in its topic partition. It identifies a record, not a delivery: a redelivery
  // has the same one.
  type TopicPartitionOffset = (TopicPartition, Long)

}

// Gives acknowledgements to the Java consumer of the records they are for. Acknowledgers with the
// same source acknowledge through the same consumer, which is what equality is based on, so
// `acknowledgeChunks` can group acknowledgements by acknowledger. The consumer records every
// acknowledgement of a chunk for a delivery it holds before it raises a `RecordNotInFlightException`
// for the first one it doesn't, which lets the pipe drop the exception for the whole chunk.
private[kafka] trait Acknowledger[F[_]] {

  def acknowledge(acks: Chunk[Acknowledgement]): F[Unit]

  // The acknowledger this one was derived from through `mapK`, or this one itself.
  def source: AnyRef = this

  final override def equals(that: Any): Boolean =
    that match {
      case that: Acknowledger[_] => source eq that.source
      case _ => false
    }

  final override def hashCode(): Int =
    System.identityHashCode(source)

}

private[kafka] object Acknowledger {

  final private class Mapped[F[_], G[_]](underlying: Acknowledger[F], f: F ~> G) extends Acknowledger[G] {

    override def acknowledge(acks: Chunk[Acknowledgement]): G[Unit] =
      f(underlying.acknowledge(acks))

    override val source: AnyRef = underlying.source

    override def toString: String = underlying.toString

  }

  def mapK[F[_], G[_]](acknowledger: Acknowledger[F], f: F ~> G): Acknowledger[G] =
    new Mapped(acknowledger, f)

}
