/*
 * Copyright 2018 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka.internal

import scala.concurrent.duration.*

import cats.effect.Temporal
import cats.syntax.all.*
import cats.Parallel
import fs2.{Chunk, Pipe, Pull}
import fs2.kafka.{AcknowledgedOffset, AcknowledgedOffsetBatch, KafkaShareCommitter}

private[kafka] object CommitPollWithin {

  /**
    * The offsets of one consumer (committer) waiting to be committed, and when the oldest of them
    * must be committed at the latest.
    */
  final private case class PendingPollAcks[F[_]](
    committer: KafkaShareCommitter[F],
    offsets: Chunk[AcknowledgedOffset[F]],
    deadline: FiniteDuration
  )

  def apply[F[_]](
    maxDelay: FiniteDuration
  )(implicit F: Temporal[F], P: Parallel[F]): Pipe[F, AcknowledgedOffset[F], Unit] =
    _.pull
      .timed { timed =>
        def commit(groups: Vector[PendingPollAcks[F]]): Pull[F, Unit, Unit] =
          if (groups.isEmpty) Pull.done
          else
            Pull.eval(
              AcknowledgedOffsetBatch.fromFoldable(groups.flatMap(_.offsets.toVector)).commit
            ) >> Pull.output1(())

        def ready(pending: PendingPollAcks[F], now: FiniteDuration): F[Boolean] =
          if (pending.deadline <= now) F.pure(true)
          else
            pending
              .committer
              .pending
              .map(
                pending.offsets.toVector.map(a => (a.topicPartition, a.offset)).distinct.size >= _
              )

        def add(now: FiniteDuration)(
          pending: Vector[PendingPollAcks[F]],
          offset: AcknowledgedOffset[F]
        ): F[Vector[PendingPollAcks[F]]] = {
          val committer = offset.committer
          pending.indexWhere(_.committer == committer) match {
            case -1 =>
              F.pure(pending :+ PendingPollAcks(committer, Chunk.singleton(offset), now + maxDelay))
            case index =>
              val group = pending(index)
              F.pure(
                pending.updated(
                  index,
                  group.copy(offsets = group.offsets ++ Chunk.singleton(offset))
                )
              )
          }
        }

        def schedule(
          pending: Vector[PendingPollAcks[F]],
          next: Pull.Timed[F, AcknowledgedOffset[F]]
        ): Pull[F, Unit, Vector[PendingPollAcks[F]]] =
          Pull
            .eval(F.monotonic)
            .flatMap { now =>
              val (due, remaining) = pending.partition(_.deadline <= now)
              if (due.nonEmpty) commit(due) >> schedule(remaining, next)
              else if (remaining.isEmpty) Pull.pure(remaining)
              else next.timeout(remaining.map(_.deadline).min - now).as(remaining)
            }

        def go(
          pending: Vector[PendingPollAcks[F]],
          timed: Pull.Timed[F, AcknowledgedOffset[F]]
        ): Pull[F, Unit, Unit] =
          timed
            .uncons
            .flatMap {
              case None =>
                commit(pending)
              case Some((Left(_), next)) =>
                step(pending, Chunk.empty, next)
              case Some((Right(chunk), next)) =>
                step(pending, chunk, next)
            }

        def step(
          pending: Vector[PendingPollAcks[F]],
          chunk: Chunk[AcknowledgedOffset[F]],
          next: Pull.Timed[F, AcknowledgedOffset[F]]
        ): Pull[F, Unit, Unit] =
          for {
            now        <- Pull.eval(F.monotonic)
            added      <- Pull.eval(chunk.foldLeftM(pending)(add(now)))
            flags      <- Pull.eval(added.traverse(ready(_, now)))
            (due, rest) = added.zip(flags).partition(_._2)
            _          <- commit(due.map(_._1))
            remaining  <- schedule(rest.map(_._1), next)
            result     <- go(remaining, next)
          } yield result

        go(Vector.empty, timed)
      }
      .stream

}
