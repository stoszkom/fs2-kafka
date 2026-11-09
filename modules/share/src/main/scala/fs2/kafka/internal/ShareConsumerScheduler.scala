/*
 * Copyright 2018 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka.internal

import cats.effect.Async
import cats.syntax.all.*
import fs2.Stream
import fs2.concurrent.Signal
import fs2.concurrent.SignallingRef
import fs2.kafka.ShareConsumerSettings
import fs2.kafka.internal.Acknowledgement.TopicPartitionOffset
import scala.concurrent.duration.*

/**
  * Runs for as long as the share consumer, acknowledging records past the max processing time,
  * renewing records, and sending acknowledgements when they are due.<br><br>
  *
  * It wakes up at the earliest deadline, which is recomputed whenever the held records or the pause
  * signal changes. Due actions run outside of that, so that a new deadline can't interrupt them. A
  * failure fails the running `consume`, and the scheduler keeps running.
  *
  * @param failure
  *   the last failure of the scheduler, which fails the running `consume`
  */
final private[kafka] class ShareConsumerScheduler[F[_], K, V] private (
  config: ShareConsumerConfig[F, K, V],
  core: ShareConsumerCore[F],
  pause: Signal[F, Boolean],
  failure: SignallingRef[F, Option[Throwable]]
)(implicit F: Async[F]) {

  import ShareConsumerScheduler.*

  private val settings: ShareConsumerSettings[F, K, V] = config.settings

  def run: F[Unit] =
    Signal
      .applicativeInstance[F]
      .map2(core.held, pause)((s, p) => actions(s, p).map(_._1).reduceOption(_ min _))
      .discrete
      .changes(cats.Eq.fromUniversalEquals[Option[FiniteDuration]])
      .switchMap {
        case None => Stream.empty
        case Some(at) =>
          Stream.eval(F.monotonic).flatMap(now => Stream.sleep_[F](at - now)) ++ Stream.emit(())
      }
      .evalMap(_ => retried(due))
      .compile
      .drain

  /**
    * Fails with the next failure of the scheduler.
    */
  def failed: F[Unit] =
    failure.discrete
      .collectFirst { case Some(error) => error }
      .compile
      .lastOrError
      .flatMap(F.raiseError[Unit])

  /**
    * Forgets the last failure, when consuming starts.
    */
  def reset: F[Unit] =
    failure.set(None)

  /**
    * Runs `step`, retrying it after a failure, as the deadline which made it due might not
    * change. Each failure fails the running `consume`.
    */
  private def retried(step: F[Unit]): F[Unit] =
    step.handleErrorWith(error => failure.set(Some(error)) *> F.sleep(retryInterval) *> retried(step))

  /**
    * Returns the actions of the scheduler with their deadlines, in the order they run when due at
    * the same time.
    */
  private def actions(s: HeldRecords, paused: Boolean): List[(FiniteDuration, F[Unit])] =
    List(
      settings.maxProcessingTime.flatMap(max => s.nextOverdue(max).tupleRight(acknowledgeOverdue(max))),
      s.nextRenewal(autoRenewalInterval(s)).tupleRight(renew),
      s.nextSend(settings.maxAcknowledgementDelay, paused).tupleRight(core.send)
    ).flatten

  private def autoRenewalInterval(s: HeldRecords): Option[FiniteDuration] =
    Some(config.renewalInterval(s.lock)).filter(_ => settings.autoRenewal)

  /**
    * Runs due actions one at a time until none is due, as an action only moves its own deadline.
    */
  private def due: F[Unit] =
    (core.held.get, pause.get, F.monotonic)
      .flatMapN { (s, p, now) =>
        actions(s, p)
          .collectFirst { case (at, action) if at <= now => action.as(true) }
          .getOrElse(F.pure(false))
      }
      .iterateWhile(identity)
      .void

  /**
    * Acknowledges records past the max processing time with the failure acknowledgement, and
    * sends the acknowledgements immediately, so that a late acknowledgement fails.
    */
  private def acknowledgeOverdue(max: FiniteDuration): F[Unit] =
    core.sendOverdue(max, settings.failureAcknowledgement)

  /**
    * Renews every unacknowledged record, along with the pending acknowledgements.<br><br>
    *
    * Renewing works as follows.
    *   1. The renewals are sent without waiting for the broker, as in Java's own renewal loop.
    *      The renewal is skipped if it is no longer due once the mutex is held, as a poll since
    *      then would otherwise have its new records renewed.
    *   2. `confirmed` checks with the Java consumer until the broker has answered.
    *   3. `core.takeBack` takes the renewed records back with a poll, which fetches nothing, as no
    *      renewal is pending anymore.
    *
    * Meanwhile, there is nothing else to send, as every unacknowledged record is being renewed,
    * and acknowledgements made in the meantime wait for their records. If the Java consumer fails
    * on the way, the records are held again as they were, to be renewed or acknowledged later,
    * rather than being left renewing forever. Nothing is renewed while unsubscribed, as Java's
    * poll would refuse to take the records back.
    */
  private def renew: F[Unit] =
    for {
      at <- F.monotonic
      subscribed <- core.subscribed
      _ <- if (subscribed) renewAt(at) else core.skipRenewal(at)
    } yield ()

  private def renewAt(at: FiniteDuration): F[Unit] = {
    val renewal =
      for {
        sent <- core.renew(at)(renewalDue(at))
        _ <- sent.traverse_ { case (offsets, lock) =>
          val forgetAt = at + lock.getOrElse(Duration.Zero) + config.apiTimeout
          confirmed(offsets, forgetAt, Set.empty).flatMap(core.takeBack)
        }
      } yield ()
    renewal.onError { case _ => core.failRenewal }
  }

  private def renewalDue(at: FiniteDuration)(s: HeldRecords): Boolean =
    s.nextRenewal(autoRenewalInterval(s)).exists(_ <= at)

  /**
    * Checks with the Java consumer, using a commit, until the broker has answered for every
    * record sent for renewal, and returns the records whose renewal was confirmed.<br><br>
    *
    * A failed check counts as no answer yet, as Java still has the renewals. If the broker can no
    * longer answer, or the consumer has unsubscribed, no records are returned, as Java's poll
    * would fetch while a renewal is pending, or throw.
    */
  private def confirmed(
    waiting: Set[TopicPartitionOffset],
    forgetAt: FiniteDuration,
    done: Set[TopicPartitionOffset]
  ): F[Set[TopicPartitionOffset]] =
    F.monotonic.flatMap { now =>
      if (waiting.isEmpty) F.pure(done)
      else if (now >= forgetAt) F.pure(Set.empty[TopicPartitionOffset])
      else
        for {
          _ <- F.sleep(renewalCheckInterval)
          check <- core.checkRenewals
          result <- check.fold(F.pure(Set.empty[TopicPartitionOffset])) { reported =>
            val answered = reported.filter { case (p, _) => waiting(p) }
            val accepted = answered.collect { case (p, true) => p }
            confirmed(waiting -- answered.map(_._1), forgetAt, done ++ accepted)
          }
        } yield result
    }

}

private[kafka] object ShareConsumerScheduler {

  def of[F[_], K, V](
    config: ShareConsumerConfig[F, K, V],
    core: ShareConsumerCore[F],
    pause: Signal[F, Boolean]
  )(implicit F: Async[F]): F[ShareConsumerScheduler[F, K, V]] =
    SignallingRef
      .of[F, Option[Throwable]](None)
      .map(new ShareConsumerScheduler(config, core, pause, _))

  /**
    * How long the scheduler waits before retrying an action which failed.
    */
  private val retryInterval: FiniteDuration = 1.second

  /**
    * How often a renewal checks with the Java consumer whether the broker has answered.
    */
  private val renewalCheckInterval: FiniteDuration = 10.millis

}
