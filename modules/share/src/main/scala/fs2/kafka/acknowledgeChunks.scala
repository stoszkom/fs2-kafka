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
  * Stages the decisions in each chunk with one call per consumer, instead of one per record. A
  * decision for a record the consumer no longer holds is skipped, as the record is delivered again.
  *
  * It pays off after steps which keep the stream's chunks, such as `map` or `evalMapChunk`, where a
  * chunk is a whole poll. After `parEvalMap`, chunks mostly hold one record; acknowledge each
  * record there, or group the decisions first with `groupWithin(n, d).unchunks`.
  */
object acknowledgeChunks {

  def apply[F[_]](implicit
    F: Concurrent[F]
  ): Pipe[F, (AcknowledgeableOffset[F], AcknowledgeType), Unit] =
    _.chunks
      .evalMap { chunk =>
        val decisions = chunk.toVector
        val stager    = decisions.headOption.map(_._1.stager)
        // Usually a chunk goes to one consumer. Offsets mapped with different `mapK` functions are
        // staged through each.
        if (decisions.forall(d => stager.contains(d._1.stager))) stage(decisions)
        else {
          val byStager = decisions.groupBy(_._1.stager).values.toList
          // Stagers are called concurrently, and the first error is raised as is.
          F.parTraverseN(byStager.size)(byStager)(stage(_).attempt).flatMap(_.sequence_.liftTo[F])
        }
      }

  private def stage[F[_]](
    decisions: Vector[(AcknowledgeableOffset[F], AcknowledgeType)]
  )(implicit F: Concurrent[F]): F[Unit] =
    decisions
      .headOption
      .traverse_(
        _._1
          .stager
          .stage(Chunk.from(decisions.map { case (offset, kind) => offset.decision(kind) }))
      )
      .recover { case _: RecordNotInFlightException => () }

}
