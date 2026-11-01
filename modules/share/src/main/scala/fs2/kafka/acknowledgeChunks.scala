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
  * Stages the decisions in each chunk with one call per consumer, instead of one per record. The
  * next poll of each consumer sends them, as in Java's poll loop. A decision for a record the
  * consumer no longer holds, for example one released after the max processing time, is skipped, as
  * the record is delivered again.
  */
object acknowledgeChunks {

  def apply[F[_]](implicit
    F: Concurrent[F]
  ): Pipe[F, (AcknowledgeableOffset[F], AcknowledgeType), Unit] =
    _.chunks
      .evalMap { chunk =>
        chunk.toVector.groupBy(_._1.stager.consumer).values.toList match {
          case Nil        => F.unit
          case one :: Nil => stage(one)
          case byConsumer =>
            // Every consumer gets its decisions at once, even if another one is busy or fails. The
            // first error is raised as is, as it may be shared with the consumer's stream.
            F.parTraverseN(byConsumer.size)(byConsumer)(stage(_).attempt)
              .flatMap(_.collectFirst { case Left(error) => error }.traverse_(F.raiseError[Unit]))
        }
      }

  private def stage[F[_]](
    decisions: Vector[(AcknowledgeableOffset[F], AcknowledgeType)]
  )(implicit F: Concurrent[F]): F[Unit] =
    decisions
      .head
      ._1
      .stager
      .stage(Chunk.from(decisions.map { case (offset, kind) => offset.decision(kind) }))
      .recover { case _: RecordNotInFlightException => () }

}
