/*
 * Copyright 2018 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka

import cats.syntax.all.*
import cats.MonadThrow
import fs2.{Chunk, Pipe}

/**
  * Acknowledges the records of each chunk with one call per run of records of the same consumer,
  * instead of one per record. A acknowledgement for a record the consumer no longer holds is
  * skipped, as the record is delivered again.
  *
  * It pays off after steps which keep the stream's chunks, such as `map` or `evalMapChunk`, where a
  * chunk is a whole poll. After `parEvalMap`, chunks mostly hold one record; acknowledge each
  * record there, or group the acknowledgements first with `groupWithin(n, d).unchunks`.
  */
object acknowledgeChunks {

  def apply[F[_]](implicit
    F: MonadThrow[F]
  ): Pipe[F, (AcknowledgeableOffset[F], AcknowledgeType), Unit] =
    // Usually a chunk goes to one consumer. Runs of offsets mapped with different `mapK` functions are
    // acknowledged through each, one after another in the chunk's order, as the last acknowledgement wins.
    _.chunks.evalMap(chunk => runs(chunk.toVector).traverse_(acknowledge(_)))

  private def runs[F[_]](
    acks: Vector[(AcknowledgeableOffset[F], AcknowledgeType)]
  ): List[Vector[(AcknowledgeableOffset[F], AcknowledgeType)]] =
    acks.foldRight(List.empty[Vector[(AcknowledgeableOffset[F], AcknowledgeType)]]) {
      case (a, run :: rest) if run.headOption.exists(_._1.acknowledger == a._1.acknowledger) =>
        (a +: run) :: rest
      case (a, rest) => Vector(a) :: rest
    }

  private def acknowledge[F[_]](
    acks: Vector[(AcknowledgeableOffset[F], AcknowledgeType)]
  )(implicit F: MonadThrow[F]): F[Unit] =
    acks
      .headOption
      .traverse_(
        _._1
          .acknowledger
          .acknowledge(Chunk.from(acks.map { case (offset, kind) => offset.ack(kind) }))
      )
      .recover { case _: RecordNotInFlightException => () }

}
