/*
 * Copyright 2018 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka

import cats.MonadThrow
import cats.Reducible
import cats.effect.Async
import cats.effect.Deferred
import cats.effect.Poll
import cats.effect.Ref
import cats.effect.Resource
import cats.effect.std.Mutex
import cats.effect.syntax.all.*
import cats.syntax.all.*
import fs2.Chunk
import fs2.Stream
import fs2.concurrent.SignallingRef
import fs2.kafka.consumer.KafkaMetrics
import fs2.kafka.consumer.KafkaShareSubscription
import fs2.kafka.consumer.MkShareConsumer
import fs2.kafka.internal.Acknowledgement
import fs2.kafka.internal.Acknowledger
import fs2.kafka.internal.Deadlines
import fs2.kafka.internal.Delivery
import fs2.kafka.internal.ShareConsumerConfig
import fs2.kafka.internal.ShareConsumerState
import fs2.kafka.internal.ShareConsumerState.TopicPartitionOffset
import fs2.kafka.internal.WithShareConsumer
import fs2.kafka.internal.converters.collection.*
import fs2.kafka.internal.converters.option.*
import fs2.kafka.internal.syntax.*
import org.apache.kafka.common.Metric
import org.apache.kafka.common.MetricName
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.TimeoutException
import scala.collection.immutable.SortedSet
import scala.concurrent.duration.*

/**
  * A share consumer which runs Java's loop: it polls, runs `process` on the poll's records, and
  * polls again once `process` returned. No poll runs while `process` runs, so acknowledgements go
  * straight into the Java consumer, under a mutex shared with the renewals and delayed sends which
  * run meanwhile.
  */
sealed abstract class KafkaShareConsumer[F[_], K, V] extends KafkaShareSubscription[F] with KafkaMetrics[F] {

  /**
    * Returns the settings used to create the consumer instance.
    */
  def settings: ShareConsumerSettings[F, K, V]

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
    * acknowledgements are sent. Stopping is final: consuming again fails with an
    * `IllegalStateException`; create a new consumer instead.
    */
  def stopConsuming: F[Unit]

}

object KafkaShareConsumer {

  def resource[F[_], K, V](
    settings: ShareConsumerSettings[F, K, V]
  )(implicit F: Async[F], mk: MkShareConsumer[F]): Resource[F, KafkaShareConsumer[F, K, V]] =
    for {
      keyDeserializer <- settings.keyDeserializer
      valueDeserializer <- settings.valueDeserializer
      config <- Resource.eval(ShareConsumerConfig.from(settings))
      withConsumer <- WithShareConsumer(mk, settings)
      consumer <-
        Resource.eval(
          (
            Mutex[F],
            SignallingRef.of[F, ShareConsumerState](ShareConsumerState.empty),
            Deferred[F, Unit],
            Ref.of[F, Boolean](false),
            Ref.of[F, Boolean](false)
          ).mapN(
            new Impl(config, keyDeserializer, valueDeserializer, withConsumer, _, _, _, _, _)
          )
        )
    } yield consumer

  /**
    * Creates `consumers` [[KafkaShareConsumer]]s in the same share group, see
    * [[KafkaShareConsumers.resource]].
    */
  def resource[F[_], K, V](settings: ShareConsumerSettings[F, K, V], consumers: Int)(implicit
    F: Async[F],
    mk: MkShareConsumer[F]
  ): Resource[F, KafkaShareConsumers[F, K, V]] = KafkaShareConsumers.resource(settings, consumers)

  def stream[F[_], K, V](settings: ShareConsumerSettings[F, K, V])(implicit
    F: Async[F],
    mk: MkShareConsumer[F]
  ): Stream[F, KafkaShareConsumer[F, K, V]] = Stream.resource(resource(settings))

  private def offsetOf(record: KafkaByteConsumerRecord): TopicPartitionOffset =
    new TopicPartition(record.topic, record.partition) -> record.offset

  private def deliveryCountOf(record: KafkaByteConsumerRecord): Short =
    record.deliveryCount().toScala.fold(1: Short)(_.shortValue)

  // The acquisition lock duration last reported by the broker.
  private def acquisitionLock(consumer: KafkaByteShareConsumer): Option[FiniteDuration] =
    consumer.acquisitionLockTimeoutMs().toScala.map(_.intValue.millis)

  // How often an unsubscribed consumer checks for a subscription.
  private val subscriptionCheckInterval: FiniteDuration = 50.millis

  // Java's `commitSync`, failing with a ShareCommitTimeoutException once the timeout passed.
  private def commitSync[F[_]](withConsumer: WithShareConsumer[F], timeout: FiniteDuration)(implicit
    F: MonadThrow[F]
  ): F[Unit] =
    withConsumer
      .blocking(consumer => consumer.commitSync(timeout.toJava): Unit)
      .adaptError { case error: TimeoutException => ShareCommitTimeoutException(timeout, error) }

  final private class Impl[F[_], K, V](
    config: ShareConsumerConfig[F, K, V],
    keyDeserializer: KeyDeserializer[F, K],
    valueDeserializer: ValueDeserializer[F, V],
    withConsumer: WithShareConsumer[F],
    mutex: Mutex[F],
    state: SignallingRef[F, ShareConsumerState],
    stop: Deferred[F, Unit],
    consuming: Ref[F, Boolean],
    // In implicit mode, after `process` failed: any poll or commit would accept its records.
    poisoned: Ref[F, Boolean]
  )(implicit F: Async[F])
    extends KafkaShareConsumer[F, K, V]
    with Acknowledger[F] {

    override val settings: ShareConsumerSettings[F, K, V] = config.settings

    override def metrics: F[Map[MetricName, Metric]] =
      withConsumer.blocking(_.metrics().asScala.toMap)

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
      else
        exclusively(
          loop((cancelable, records, start, lock) => processed(cancelable, records, start, lock, process))
        )

    override def autoConsume(process: Chunk[ConsumerRecord[K, V]] => F[Unit]): F[Unit] =
      if (!config.implicitMode)
        F.raiseError(
          new IllegalStateException("autoConsume needs share.acknowledgement.mode=implicit")
        )
      else
        exclusively(
          loop { case (cancelable, records, _, _) =>
            // Accepting at once, rather than with the next poll, also covers a pause after it.
            cancelable(
              Chunk
                .from(records)
                .traverse(ConsumerRecord.fromJava(_, keyDeserializer, valueDeserializer))
                .flatMap(process)
            )
              // Also when canceled, e.g. as another of several consumers failed.
              .guaranteeCase(outcome => poisoned.set(true).unlessA(outcome.isSuccess)) *>
              withConsumer.blocking(_.commitAsync())
          }
        )

    private def exclusively(run: F[Unit]): F[Unit] =
      poisoned.get
        .ifM(
          F.raiseError(
            new IllegalStateException(
              "process failed in implicit mode: close the consumer, which releases its records"
            )
          ),
          stop.tryGet
            .flatMap {
              case Some(_) => F.raiseError(new IllegalStateException("the consumer was stopped"))
              case None => consumingOnce(run)
            }
        )

    // Uncancelable from taking the flag until its reset is in place, or a cancel in between would
    // leave the consumer "already consuming" for good.
    private def consumingOnce(run: F[Unit]): F[Unit] =
      F.uncancelable { cancelable =>
        consuming
          .getAndSet(true)
          .ifM(
            F.raiseError(new IllegalStateException("the consumer is already consuming")),
            cancelable(
              withConsumer
                .blocking(_.subscription().isEmpty)
                .ifM(F.raiseError(NotSubscribedException()), run)
            ).guarantee(consuming.set(false))
          )
      }

    // Polls until stopped, waiting while paused, and gives each poll to `run` with when it started
    // and the lock the broker reported. A poll can't be canceled until `run` has taken its records:
    // Java holds them from the poll on, and a cancel in between would leave them unknown here. `run`
    // lets the processing be canceled again with the `Poll` it gets.
    private def loop(
      run: (
        Poll[F],
        Vector[KafkaByteConsumerRecord],
        FiniteDuration,
        Option[FiniteDuration]
      ) => F[Unit]
    ): F[Unit] = {
      // A stop already requested wins: racing it against a wait which is already complete could pick
      // the wait and poll again.
      lazy val next: F[Unit] =
        stop.tryGet
          .flatMap {
            case Some(_) => F.pure(().asLeft[Unit])
            case None => F.race(stop.get, unpaused)
          }
          .flatMap {
            case Left(_) => finalCommit
            case Right(_) =>
              F.uncancelable { cancelable =>
                F.monotonic.flatMap { start =>
                  poll.flatMap {
                    case None => F.pure(false)
                    case Some((records, lock)) =>
                      run(cancelable, records, start, lock).whenA(records.nonEmpty).as(true)
                  }
                }
              }.flatMap(polled => F.sleep(subscriptionCheckInterval).unlessA(polled) *> next)
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
      commitSync(withConsumer, settings.commitTimeout)

    // --- One poll in explicit mode ---

    private def processed(
      cancelable: Poll[F],
      records: Vector[KafkaByteConsumerRecord],
      start: FiniteDuration,
      lock: Option[FiniteDuration],
      process: Chunk[AcknowledgeableConsumerRecord[F, K, V]] => F[Unit]
    ): F[Unit] =
      F.monotonic
        .flatMap { now =>
          val deliveries = records.map(r => offsetOf(r) -> Delivery(r, deliveryCountOf(r), now))
          // Also when decoding fails or canceled: the poll is held from the state update on.
          state
            .modify(ShareConsumerState.polled(deliveries, start, lock, config.apiTimeout, _))
            .flatMap { returned =>
              val fresh =
                if (returned.isEmpty) records
                else {
                  val offsets = returned.map(offsetOf).toSet
                  records.filterNot(r => offsets(offsetOf(r)))
                }
              releaseAtOnce(returned) *>
                cancelable(
                  Chunk
                    .from(fresh)
                    .traverse(decode)
                    .flatMap { chunk =>
                      // The timers only end by failing, which cancels `process` and fails `consume`.
                      F.race(F.both(timers, sendsWhenPaused) *> F.never[Unit], process(chunk)).void
                    }
                ).whenA(fresh.nonEmpty)
            }
            .guaranteeCase(outcome => mutex.lock.surround(releaseUnacked(ended = !outcome.isSuccess)))
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

    // Records a poll returned which `process` must not get: released and sent at once.
    private def releaseAtOnce(records: Vector[KafkaByteConsumerRecord]): F[Unit] =
      withConsumer
        .blocking { consumer =>
          records.foreach(consumer.acknowledge(_, AcknowledgeType.Release.asJava))
          consumer.commitAsync()
        }
        .whenA(records.nonEmpty)

    // The records `process` left are released, sent with the next poll, or at once while paused or
    // when `consume` ended, as no poll follows. The poll is then closed, so that a late
    // acknowledgement fails rather than replace a release.
    private def releaseUnacked(ended: Boolean): F[Unit] =
      state.get
        .flatMap { s =>
          release(s.unacked) *> (if (ended) send else paused.ifM(send, F.unit)) *>
            state.update(_.closed)
        }

    // --- Acknowledgements ---

    // The last acknowledgement of a delivery wins, as in Java. Uncancelable once the mutex is held:
    // a cancel after Java took an acknowledgement, but before the state did, would let a release
    // replace it, and after a renewal it would leave the records the renewal returned unknown here.
    override def acknowledge(acks: Chunk[Acknowledgement]): F[Unit] =
      mutex.lock
        .surround(
          state.get.flatMap { s =>
            val last = acks.toVector.map(a => a.offset -> a).toMap.values.toVector
            val (held, notHeld) = last.partition(a => s.holds(a.offset, a.deliveryCount))
            val (renewals, now) = held.partition(_.acknowledgeType == AcknowledgeType.Renew)
            recordInJava(s, now.map(a => a.offset -> a.acknowledgeType)).flatMap { gone =>
              renew(renewals.map(_.offset).toSet).whenA(renewals.nonEmpty) *>
                paused.ifM(send, F.unit) *>
                (notHeld.map(_.offset) ++ gone).headOption
                  .traverse_(p => F.raiseError[Unit](RecordNotInFlightException(p._1, p._2)))
            }
          }.uncancelable
        )

    // Gives the acknowledgements to Java, returning those Java no longer holds, which are dropped.
    private def recordInJava(
      s: ShareConsumerState,
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
      state.get
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
      * records count as lost; should Java complete the renewal later, the poll returning them
      * releases them at once. The wait is the renewal margin, at most half the lock.
      */
    private def renew(byUser: Set[TopicPartitionOffset]): F[Unit] =
      (state.get, F.monotonic).tupled
        .flatMap { case (s, at) =>
          val renewing = (s.unacked.toSet ++ byUser).toVector
          val timeout = config.renewalWait(s.lock)
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
              val asked = renewing.toSet -- failed
              val (returned, stray) = polled.partition { r =>
                val p = offsetOf(r)
                asked(p) && s.holds(p, deliveryCountOf(r))
              }
              state.update(
                _.withRenewed(returned.map(r => offsetOf(r) -> r).toMap, byUser, at, lock)
              ) *> releaseAtOnce(stray)
            }
        }

    // --- Timers, while a poll is processed ---

    // Wakes at the earliest of the renewal, the delayed send and the max processing time, and does
    // what is due. The deadline is recomputed when the state changes it.
    private def timers: F[Unit] =
      state.discrete
        .map(nextDeadline)
        .changes(cats.Eq.fromUniversalEquals[Option[FiniteDuration]])
        .switchMap {
          case None => Stream.empty
          case Some(at) =>
            // `due` changes the deadline, which switches to a new inner stream: uncancelable, so
            // that the switch can't stop it between a release and its send.
            Stream.eval(F.monotonic).flatMap(now => Stream.sleep_[F](at - now)) ++
              Stream.exec(mutex.lock.surround(due).uncancelable)
        }
        .compile
        .drain

    // The one place the timers' deadlines are computed, so that a wake-up always finds what is due.
    private def deadlines(s: ShareConsumerState): Deadlines =
      Deadlines(
        renewal =
          if (settings.autoRenewal) s.nextRenewal(config.renewalInterval(s.lock)) else None,
        send = settings.maxAcknowledgementDelay.flatMap(s.nextSend),
        overdue = settings.maxProcessingTime.flatMap(s.nextOverdue)
      )

    private def nextDeadline(s: ShareConsumerState): Option[FiniteDuration] = deadlines(s).next

    // Does one due thing at a time and looks again, since what is done moves only its own deadline:
    // a renewal due together with an overdue release would otherwise wait for the next state change,
    // as an unchanged deadline doesn't re-arm the timer.
    private def due: F[Unit] =
      (state.get, F.monotonic).tupled
        .flatMap { case (s, now) =>
          val d = deadlines(s)
          // Overdue records are released and sent at once, so that a late acknowledgement can't replace it.
          if (d.overdue.exists(_ <= now))
            settings.maxProcessingTime.traverse_(max => release(s.overdue(max, now))) *> send *> due
          else if (d.renewal.exists(_ <= now)) renew(Set.empty) *> due
          else if (d.send.exists(_ <= now)) send *> due
          else F.unit
        }

    // While paused, no poll sends acknowledgements: those pending as the pause starts are sent then.
    private def sendsWhenPaused: F[Unit] =
      settings.pauseWhen
        .traverse_(
          _.discrete.filter(identity).evalMap(_ => mutex.lock.surround(send)).compile.drain
        )

  }

}
