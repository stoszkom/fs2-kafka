/*
 * Copyright 2018 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka.internal.actor

import scala.concurrent.duration.FiniteDuration

import fs2.kafka.internal.actor.KafkaShareConsumerActor.CommitResult
import fs2.Chunk

// A request to the share consumer's actor.
sealed abstract private[kafka] class ShareRequest[F[_]]

private[kafka] object ShareRequest {

  final case class Start[F[_]]() extends ShareRequest[F]

  final case class Stop[F[_]]() extends ShareRequest[F]

  final case class ReaderClosed[F[_]]() extends ShareRequest[F]

  final case class HandedOut[F[_]](pollId: Long) extends ShareRequest[F]

  final case class Stage[F[_]](
    decisions: Chunk[Decision],
    done: Either[Throwable, Unit] => F[Unit]
  ) extends ShareRequest[F]

  final case class CommitSync[F[_]](
    timeout: FiniteDuration,
    done: Either[Throwable, CommitResult] => F[Unit]
  ) extends ShareRequest[F]

  final case class CommitAsync[F[_]](done: Either[Throwable, Unit] => F[Unit])
      extends ShareRequest[F]

}
