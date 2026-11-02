/*
 * Copyright 2018 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka

import cats.effect.Concurrent
import cats.syntax.all.*
import fs2.{Chunk, Pipe}

/**
  * Acknowledges the records of each chunk with one call per consumer, instead of one per record. A
  * acknowledgement for a record the consumer no longer holds is skipped, as the record is delivered
  * again.
  *
  * It pays off after steps which keep the stream's chunks, such as `map` or `evalMapChunk`, where a
  * chunk is a whole poll. After `parEvalMap`, chunks mostly hold one record; acknowledge each
  * record there, or group the acknowledgements first with `groupWithin(n, d).unchunks`.
  */
object acknowledgeChunks {

  def apply[F[_]](implicit
    F: Concurrent[F]
  ): Pipe[F, (AcknowledgeableOffset[F], AcknowledgeType), Unit] =
    _.chunks
      .evalMap { chunk =>
        val acks         = chunk.toVector
        val acknowledger = acks.headOption.map(_._1.acknowledger)
        // Usually a chunk goes to one consumer. Offsets mapped with different `mapK` functions are
        // acknowledged through each.
        if (acks.forall(a => acknowledger.contains(a._1.acknowledger))) acknowledge(acks)
        else {
          val byAcknowledger = acks.groupBy(_._1.acknowledger).values.toList
          // Acknowledgers are called concurrently, and the first error is raised as is.
          F.parTraverseN(byAcknowledger.size)(byAcknowledger)(acknowledge(_).attempt)
            .flatMap(_.sequence_.liftTo[F])
        }
      }

  private def acknowledge[F[_]](
    acks: Vector[(AcknowledgeableOffset[F], AcknowledgeType)]
  )(implicit F: Concurrent[F]): F[Unit] =
    acks
      .headOption
      .traverse_(
        _._1
          .acknowledger
          .acknowledge(Chunk.from(acks.map { case (offset, kind) => offset.ack(kind) }))
      )
      .recover { case _: RecordNotInFlightException => () }

}
