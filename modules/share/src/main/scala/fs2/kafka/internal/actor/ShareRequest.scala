/*
 * Copyright 2018 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka.internal.actor

import fs2.Chunk

// A request to the share consumer's actor.
sealed abstract private[kafka] class ShareRequest[F[_]]

private[kafka] object ShareRequest {

  final case class Start[F[_]]() extends ShareRequest[F]

  final case class Stop[F[_]]() extends ShareRequest[F]

  final case class ReaderClosed[F[_]]() extends ShareRequest[F]

  final case class HandedOut[F[_]](pollId: Long) extends ShareRequest[F]

  // The pause signal changed.
  final case class Paused[F[_]](paused: Boolean) extends ShareRequest[F]

  final case class Ack[F[_]](
    acks: Chunk[Acknowledgement],
    done: Either[Throwable, Unit] => F[Unit]
  ) extends ShareRequest[F]

}
