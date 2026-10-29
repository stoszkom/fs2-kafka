/*
 * Copyright 2018 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka

import scala.concurrent.duration.FiniteDuration

import cats.effect.Temporal
import cats.Parallel
import fs2.Pipe

/**
  * Commits the acknowledged offsets of share consumer records, separately for each consumer. The
  * offsets are commited as soon as either of two happens:
  *   1. Every record of the latest poll of the consumer has been acknowledged.
  *   2. The maxDelay has passed since the first acknowledged offset arrived
  *
  * It's recommended to keep `maxDelay` short enough that the waiting offsets are committed before
  * their acquisition locks need renewing, or expire with auto-renewal disabled. In implicit
  * acknowledgement mode, it fails the stream with an [[ImplicitAcknowledgementException]].
  */
object commitPollWithin {

  def apply[F[_]: Parallel: Temporal](
    maxDelay: FiniteDuration
  ): Pipe[F, AcknowledgedOffset[F], Unit] =
    internal.CommitPollWithin(maxDelay)

}
