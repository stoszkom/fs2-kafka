/*
 * Copyright 2018 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka.sync

import scala.collection.immutable.SortedSet
import scala.concurrent.duration.*

import cats.effect.{Async, Deferred, Ref, Resource}
import cats.effect.std.Mutex
import cats.effect.syntax.all.*
import cats.syntax.all.*
import cats.Reducible
import fs2.{Chunk, Stream}
import fs2.concurrent.SignallingRef
import fs2.kafka.*
import fs2.kafka.consumer.{KafkaShareSubscription, MkShareConsumer}
import fs2.kafka.internal.actor.{
  Acknowledgement,
  Acknowledger,
  ShareActorConfig,
  TopicPartitionOffset
}
import fs2.kafka.internal.actor.KafkaShareConsumerActor.{acquisitionLock, deliveryCountOf, offsetOf}
import fs2.kafka.internal.converters.collection.*
import fs2.kafka.internal.syntax.*
import fs2.kafka.internal.WithShareConsumer

import org.apache.kafka.common.errors.TimeoutException

/**
  * A share consumer which runs Java's loop: it polls, runs `process` on the poll's records, and
  * polls again once `process` returned. No poll runs while `process` runs, so acknowledgements go
  * straight into the Java consumer, under a mutex shared with the renewals and delayed sends which
  * run meanwhile.
  */
sealed abstract class SyncShareConsumer[F[_], K, V] extends KafkaShareSubscription[F] {

  /**
    * Runs `process` on each poll, one poll at a time, until [[stopConsuming]], in explicit
    * acknowledgement mode. `process` may work on the records in parallel and acknowledge each as it
    * finishes. Acknowledgements are sent after the `maxAcknowledgementDelay`, at once while paused,
    * or with the next poll. Records `process` leaves unacknowledged are released once it returns;
    * if it fails, they are released and the error is raised. Acknowledging a record after `process`
    * returned fails with a [[RecordNotInFlightException]].
    */
  def consume(process: Chunk[AcknowledgeableConsumerRecord[F, K, V]] => F[Unit]): F[Unit]

  /**
    * Runs `process` on each poll until [[stopConsuming]], in implicit acknowledgement mode: once
    * `process` returns, the poll's records are accepted. If `process` fails, they are not: closing
    * the consumer releases them, so they are delivered again, as in Java's loop. As Java would
    * accept them with its next poll, the consumer then refuses to consume again; close it and
    * create a new one.
    */
  def autoConsume(process: Chunk[ConsumerRecord[K, V]] => F[Unit]): F[Unit]

  /**
    * Makes [[consume]] or [[autoConsume]] return once the poll being processed is done and the last
    * acknowledgements are sent.
    */
  def stopConsuming: F[Unit]

}

object SyncShareConsumer {

  def resource[F[_], K, V](
    settings: ShareConsumerSettings[F, K, V]
  )(implicit F: Async[F], mk: MkShareConsumer[F]): Resource[F, SyncShareConsumer[F, K, V]] =
    for {
      keyDeserializer   <- settings.keyDeserializer
      valueDeserializer <- settings.valueDeserializer
      config            <- Resource.eval(ShareActorConfig.from(settings))
      withConsumer      <- WithShareConsumer(mk, settings)
      consumer          <-
        Resource.eval(
          (
            Mutex[F],
            SignallingRef.of[F, SyncState](SyncState.empty),
            Deferred[F, Unit],
            Ref.of[F, Boolean](false),
            Ref.of[F, Boolean](false)
          ).mapN(
            new Impl(config, keyDeserializer, valueDeserializer, withConsumer, _, _, _, _, _)
          )
        )
    } yield consumer

  def stream[F[_], K, V](settings: ShareConsumerSettings[F, K, V])(implicit
    F: Async[F],
    mk: MkShareConsumer[F]
  ): Stream[F, SyncShareConsumer[F, K, V]] = Stream.resource(resource(settings))

  // How often an unsubscribed consumer checks for a subscription.
  private val subscriptionCheckInterval: FiniteDuration = 50.millis

  // How often records are renewed before the broker reported the lock.
  private val unknownLockRenewalInterval: FiniteDuration = 500.millis

  final private class Impl[F[_], K, V](
    config: ShareActorConfig[F, K, V],
    keyDeserializer: KeyDeserializer[F, K],
    valueDeserializer: ValueDeserializer[F, V],
    withConsumer: WithShareConsumer[F],
    mutex: Mutex[F],
    state: SignallingRef[F, SyncState],
    stop: Deferred[F, Unit],
    consuming: Ref[F, Boolean],
    // In implicit mode, after `process` failed: any poll or commit would accept its records.
    poisoned: Ref[F, Boolean]
  )(implicit F: Async[F])
      extends SyncShareConsumer[F, K, V]
      with Acknowledger[F] {

    private val settings = config.settings

    // --- Subscription ---

    override def subscribe[G[_]: Reducible](topics: G[String]): F[Unit] =
      withConsumer.blocking(_.subscribe(topics.toList.asJava))

    override def unsubscribe: F[Unit] = withConsumer.blocking(_.unsubscribe())

    override def subscription: F[SortedSet[String]] =
      withConsumer.blocking(_.subscription().toSortedSet)

    override def stopConsuming: F[Unit] = stop.complete(()).void

    // --- The loop ---

    override def consume(
      process: Chunk[AcknowledgeableConsumerRecord[F, K, V]] => F[Unit]
    ): F[Unit] =
      if (config.implicitMode) F.raiseError(ImplicitAcknowledgementException())
      else exclusively(loop((records, start) => processed(records, start, process)))

    override def autoConsume(process: Chunk[ConsumerRecord[K, V]] => F[Unit]): F[Unit] =
      if (!config.implicitMode)
        F.raiseError(
          new IllegalStateException("autoConsume needs share.acknowledgement.mode=implicit")
        )
      else
        exclusively(
          loop { case (records, _) =>
            // Accepting at once, rather than with the next poll, also covers a pause after it.
            Chunk
              .from(records)
              .traverse(ConsumerRecord.fromJava(_, keyDeserializer, valueDeserializer))
              .flatMap(process)
              // Also when canceled, e.g. as another of several consumers failed.
              .guaranteeCase(outcome => poisoned.set(true).unlessA(outcome.isSuccess)) *>
              withConsumer.blocking(_.commitAsync())
          }
        )

    private def exclusively(run: F[Unit]): F[Unit] =
      poisoned
        .get
        .ifM(
          F.raiseError(
            new IllegalStateException(
              "process failed in implicit mode: close the consumer, which releases its records"
            )
          ),
          consumingOnce(run)
        )

    private def consumingOnce(run: F[Unit]): F[Unit] =
      consuming
        .getAndSet(true)
        .ifM(
          F.raiseError(new IllegalStateException("the consumer is already consuming")),
          withConsumer
            .blocking(_.subscription().isEmpty)
            .ifM(F.raiseError(NotSubscribedException()), run)
            .guarantee(consuming.set(false))
        )

    // Polls until stopped, waiting while paused, and gives each poll to `run` with when it started.
    private def loop(run: (Vector[KafkaByteConsumerRecord], FiniteDuration) => F[Unit]): F[Unit] = {
      lazy val next: F[Unit] =
        F.race(stop.get, unpaused)
          .flatMap {
            case Left(_)  => finalCommit
            case Right(_) =>
              F.monotonic
                .flatMap { start =>
                  poll.flatMap {
                    case None                  => F.sleep(subscriptionCheckInterval) *> next
                    case Some((records, lock)) =>
                      state.update(_.withLock(lock)) *>
                        run(records, start).whenA(records.nonEmpty) *> next
                  }
                }
          }
      next
    }

    private def unpaused: F[Unit] = settings.pauseWhen.traverse_(_.waitUntil(!_))

    private def paused: F[Boolean] = settings.pauseWhen.fold(F.pure(false))(_.get)

    // Unsubscribed: nothing to poll; Java would throw.
    private def poll: F[Option[(Vector[KafkaByteConsumerRecord], Option[FiniteDuration])]] =
      withConsumer.blocking { consumer =>
        if (consumer.subscription().isEmpty) None
        else
          Some(
            consumer.poll(settings.pollTimeout.toJava).iterator().asScala.toVector ->
              acquisitionLock(consumer)
          )
      }

    private def finalCommit: F[Unit] =
      withConsumer
        .blocking(consumer => consumer.commitSync(settings.commitTimeout.toJava): Unit)
        .adaptError { case error: TimeoutException =>
          ShareCommitTimeoutException(settings.commitTimeout, error)
        }

    // --- One poll in explicit mode ---

    private def processed(
      records: Vector[KafkaByteConsumerRecord],
      start: FiniteDuration,
      process: Chunk[AcknowledgeableConsumerRecord[F, K, V]] => F[Unit]
    ): F[Unit] =
      F.monotonic
        .flatMap { now =>
          val deliveries = records.map(r => offsetOf(r) -> Delivery(r, deliveryCountOf(r), now))
          state.update(SyncState.polled(deliveries, start, None, _)) *>
            Chunk
              .from(records)
              .traverse(decode)
              .flatMap { chunk =>
                F.both(timers, sendsWhenPaused)
                  .background
                  .surround(process(chunk))
                  .guarantee(mutex.lock.surround(releaseUnacked))
              }
        }

    private def decode(record: KafkaByteConsumerRecord): F[AcknowledgeableConsumerRecord[F, K, V]] =
      ConsumerRecord
        .fromJava(record, keyDeserializer, valueDeserializer)
        .map { decoded =>
          val (partition, offset) = offsetOf(record)
          AcknowledgeableConsumerRecord(
            decoded,
            AcknowledgeableOffset(partition, offset, deliveryCountOf(record), this)
          )
        }

    // The records `process` left are released, sent at once while paused, or else with the next poll.
    // The poll is then closed, so that a late acknowledgement fails rather than replace a release.
    private def releaseUnacked: F[Unit] =
      state
        .get
        .flatMap { s =>
          release(s.unacked) *> paused.ifM(send, F.unit) *> state.update(_.closed)
        }

    // --- Acknowledgements ---

    // The last acknowledgement of a delivery wins, as in Java.
    override def acknowledge(acks: Chunk[Acknowledgement]): F[Unit] =
      mutex
        .lock
        .surround(
          state
            .get
            .flatMap { s =>
              val last            = acks.toVector.map(a => a.offset -> a).toMap.values.toVector
              val (held, notHeld) = last.partition(a => s.holds(a.offset, a.deliveryCount))
              val (renewals, now) = held.partition(_.acknowledgeType == AcknowledgeType.Renew)
              recordInJava(s, now.map(a => a.offset -> a.acknowledgeType)).flatMap { gone =>
                renew(renewals.map(_.offset).toSet).whenA(renewals.nonEmpty) *>
                  paused.ifM(send, F.unit) *>
                  (notHeld.map(_.offset) ++ gone)
                    .headOption
                    .traverse_(p => F.raiseError[Unit](RecordNotInFlightException(p._1, p._2)))
              }
            }
        )

    // Gives the acknowledgements to Java, returning those Java no longer holds, which are dropped.
    private def recordInJava(
      s: SyncState,
      acks: Vector[(TopicPartitionOffset, AcknowledgeType)]
    ): F[Vector[TopicPartitionOffset]] =
      if (acks.isEmpty) F.pure(Vector.empty)
      else
        withConsumer
          .blocking { consumer =>
            acks.map { case (p, kind) =>
              p -> Either.catchOnly[IllegalStateException](
                consumer.acknowledge(s.record(p), kind.asJava)
              )
            }
          }
          .flatMap { results =>
            val gone = results.collect { case (p, Left(_)) => p }
            F.monotonic
              .flatMap(at =>
                state.update(
                  _.withAcked(results.collect { case (p, Right(_)) => p }, at).withoutHeld(gone)
                )
              )
              .as(gone)
          }

    private def release(offsets: Vector[TopicPartitionOffset]): F[Unit] =
      state.get.flatMap(s => recordInJava(s, offsets.map(_ -> AcknowledgeType.Release))).void

    private def send: F[Unit] =
      state
        .get
        .flatMap(s =>
          (withConsumer.blocking(_.commitAsync()) *> state.update(_.withoutAcked)).whenA(
            s.hasUnsent
          )
        )

    /**
      * Renews every unacknowledged record and those the user renewed, waits for the broker, then
      * takes them back with a poll, which fetches nothing as no renewal is pending. Atomic under
      * the mutex, so an acknowledgement never sees a record being renewed. A renewal still pending
      * after the wait makes that poll fetch: new records it returns are released, and the renewed
      * records count as lost. The wait is the renewal margin, at most half the lock.
      */
    private def renew(byUser: Set[TopicPartitionOffset]): F[Unit] =
      (state.get, F.monotonic)
        .tupled
        .flatMap { case (s, at) =>
          val renewing = (s.unacked.toSet ++ byUser).toVector
          val timeout  = s
            .lock
            .fold(settings.renewalMargin)(lock => settings.renewalMargin.min(lock / 2))
          withConsumer
            .blocking { consumer =>
              val failed = renewing.filter { p =>
                Either
                  .catchOnly[IllegalStateException](
                    consumer.acknowledge(s.record(p), AcknowledgeType.Renew.asJava)
                  )
                  .isLeft
              }
              Either
                .catchOnly[TimeoutException](consumer.commitSync(timeout.toJava))
                .void
                .getOrElse(())
              (
                failed,
                consumer.poll(_root_.java.time.Duration.ZERO).iterator().asScala.toVector,
                acquisitionLock(consumer)
              )
            }
            .flatMap { case (failed, polled, lock) =>
              val asked             = renewing.toSet -- failed
              val (returned, stray) = polled.partition { r =>
                val p = offsetOf(r)
                asked(p) && s.holds(p, deliveryCountOf(r))
              }
              state.update(
                _.withRenewed(returned.map(r => offsetOf(r) -> r).toMap, byUser, at, lock)
              ) *>
                withConsumer
                  .blocking { consumer =>
                    stray.foreach(consumer.acknowledge(_, AcknowledgeType.Release.asJava))
                    consumer.commitAsync()
                  }
                  .whenA(stray.nonEmpty)
            }
        }

    // --- Timers, while a poll is processed ---

    // Wakes at the earliest of the renewal, the delayed send and the max processing time, and does
    // what is due. The deadline is recomputed when the state changes it.
    private def timers: F[Unit] =
      state
        .discrete
        .map(nextDeadline)
        .changes(cats.Eq.fromUniversalEquals[Option[FiniteDuration]])
        .switchMap {
          case None     => Stream.empty
          case Some(at) =>
            Stream.eval(F.monotonic).flatMap(now => Stream.sleep_[F](at - now)) ++
              Stream.exec(mutex.lock.surround(due))
        }
        .compile
        .drain

    private def nextDeadline(s: SyncState): Option[FiniteDuration] =
      List(
        if (settings.autoRenewal) s.nextRenewal(renewalInterval(s.lock)) else None,
        settings.maxAcknowledgementDelay.flatMap(s.nextSend),
        settings.maxProcessingTime.flatMap(s.nextOverdue)
      ).flatten.reduceOption(_ min _)

    private def due: F[Unit] =
      (state.get, F.monotonic)
        .tupled
        .flatMap { case (s, now) =>
          val overdue = settings
            .maxProcessingTime
            .fold(Vector.empty[TopicPartitionOffset])(s.overdue(_, now))
          val renewal = settings.autoRenewal && s
            .nextRenewal(renewalInterval(s.lock))
            .exists(_ <= now)
          val sending = settings.maxAcknowledgementDelay.flatMap(s.nextSend).exists(_ <= now)
          // Overdue records are released and sent at once, so that a late acknowledgement can't replace it.
          if (overdue.nonEmpty) release(overdue) *> send
          else if (renewal) renew(Set.empty)
          else if (sending) send
          else F.unit
        }

    // While paused, no poll sends acknowledgements: those pending as the pause starts are sent then.
    private def sendsWhenPaused: F[Unit] =
      settings
        .pauseWhen
        .traverse_(
          _.discrete.filter(identity).evalMap(_ => mutex.lock.surround(send)).compile.drain
        )

    // The renewal margin before the lock ends, or half the lock, early enough for
    // `max.poll.interval.ms`.
    private def renewalInterval(lock: Option[FiniteDuration]): FiniteDuration =
      lock.fold(unknownLockRenewalInterval) { lock =>
        val byMargin =
          if (settings.renewalMargin < lock) lock - settings.renewalMargin else lock / 2
        byMargin.min(config.maxPollInterval / 2)
      }

  }

}
