/*
 * Copyright 2018 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka

import cats.Reducible
import cats.effect.Async
import cats.effect.Poll
import cats.effect.Ref
import cats.effect.Resource
import cats.effect.std.Dispatcher
import cats.effect.std.Mutex
import cats.effect.syntax.all.*
import cats.syntax.all.*
import fs2.Chunk
import fs2.Stream
import fs2.concurrent.Signal
import fs2.concurrent.SignallingRef
import fs2.kafka.consumer.KafkaMetrics
import fs2.kafka.consumer.KafkaShareSubscription
import fs2.kafka.consumer.MkShareConsumer
import fs2.kafka.internal.Acknowledgement
import fs2.kafka.internal.AcknowledgementResults
import fs2.kafka.internal.AcknowledgementResults.Results
import fs2.kafka.internal.Acknowledger
import fs2.kafka.internal.Delivery
import fs2.kafka.internal.ShareConsumerConfig
import fs2.kafka.internal.ShareConsumerState
import fs2.kafka.internal.ShareConsumerState.offsetOf
import fs2.kafka.internal.Acknowledgement.TopicPartitionOffset
import fs2.kafka.internal.WithShareConsumer
import fs2.kafka.internal.converters.collection.*
import fs2.kafka.internal.converters.option.*
import fs2.kafka.internal.syntax.*
import org.apache.kafka.common.Metric
import org.apache.kafka.common.MetricName
import org.apache.kafka.common.errors.TimeoutException
import scala.collection.immutable.SortedSet
import scala.concurrent.duration.*

/**
  * A share consumer which runs Java's loop: it polls, runs `process` on the poll's records, and
  * polls again once `process` returned. Acknowledgements are recorded by the consumer; a driver,
  * which runs as long as the consumer, gives them to Java and sends them when due, renews, and
  * releases records past the max processing time. What is left goes with the next poll.<br><br>
  *
  * Experimental: this API targets Kafka's share groups.
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
    * returned fails with a [[RecordNotInFlightException]].<br><br>
    *
    * The last acknowledgements are sent before it returns, and it fails if the broker rejects them.
    * Those sent earlier, without waiting, can fail unnoticed; their records are delivered again.
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
      // Before the Java consumer, as closing it can still run the commit callback.
      dispatcher <- Dispatcher.sequential[F]
      withConsumer <- WithShareConsumer(mk, settings)
      results <- Resource.eval(Ref.of[F, Results](Vector.empty))
      // Only explicit mode renews, and reads what the callback reports.
      _ <- Resource.eval(
        withConsumer
          .blocking(_.setAcknowledgementCommitCallback(AcknowledgementResults.callback(dispatcher, results)))
          .unlessA(config.implicitMode)
      )
      consumer <- Resource.eval(
        (
          Mutex[F],
          SignallingRef.of[F, ShareConsumerState](ShareConsumerState.empty),
          SignallingRef.of[F, Lifecycle](Idle),
          SignallingRef.of[F, Option[Throwable]](None)
        ).mapN(new Impl(config, keyDeserializer, valueDeserializer, withConsumer, results, _, _, _, _))
      )
      // Stopped before the Java consumer closes.
      _ <- consumer.driver.background
    } yield consumer

  def stream[F[_], K, V](settings: ShareConsumerSettings[F, K, V])(implicit
    F: Async[F],
    mk: MkShareConsumer[F]
  ): Stream[F, KafkaShareConsumer[F, K, V]] = Stream.resource(resource(settings))

  private def deliveryCountOf(record: KafkaByteConsumerRecord): Short =
    record.deliveryCount().toScala.fold(1: Short)(_.shortValue)

  // The acquisition lock duration last reported by the broker.
  private def acquisitionLock(consumer: KafkaByteShareConsumer): Option[FiniteDuration] =
    consumer.acquisitionLockTimeoutMs().toScala.map(_.intValue.millis)

  private def pollRecords(
    consumer: KafkaByteShareConsumer,
    timeout: FiniteDuration
  ): (Vector[KafkaByteConsumerRecord], Option[FiniteDuration]) =
    consumer.poll(timeout.toJava).iterator().asScala.toVector -> acquisitionLock(consumer)

  // Gives Java every acknowledgement, then throws the first error other than its refusal of a
  // record it no longer holds: they were taken from the state, so one failure must not lose the rest.
  private def recordIn(
    consumer: KafkaByteShareConsumer,
    acks: Vector[(KafkaByteConsumerRecord, AcknowledgeType)]
  ): Unit =
    acks
      .map { case (record, kind) => Either.catchNonFatal(consumer.acknowledge(record, kind.asJava)) }
      .collectFirst { case Left(error) if !error.isInstanceOf[IllegalStateException] => error }
      .foreach(error => throw error)

  // Whether Java took the renewal: it refuses a record it no longer holds.
  private def renewedIn(consumer: KafkaByteShareConsumer, record: KafkaByteConsumerRecord): Boolean =
    Either
      .catchOnly[IllegalStateException](consumer.acknowledge(record, AcknowledgeType.Renew.asJava))
      .isRight

  // How often an unsubscribed consumer checks for a subscription.
  private val subscriptionCheckInterval: FiniteDuration = 50.millis

  // How often a renewal asks Java whether the broker answered.
  private val renewalCheckInterval: FiniteDuration = 10.millis

  // A poll's records, when it started, and the lock the broker reported.
  final private case class Polled(
    records: Vector[KafkaByteConsumerRecord],
    start: FiniteDuration,
    lock: Option[FiniteDuration]
  )

  // Whether the consumer may consume: stopping is final, and in implicit mode, once `process`
  // failed, the consumer is poisoned, as any poll or commit would accept its records.
  sealed abstract private class Lifecycle
  private case object Idle extends Lifecycle
  private case object Consuming extends Lifecycle
  private case object Stopped extends Lifecycle
  private case object Poisoned extends Lifecycle

  final private class Impl[F[_], K, V](
    config: ShareConsumerConfig[F, K, V],
    keyDeserializer: KeyDeserializer[F, K],
    valueDeserializer: ValueDeserializer[F, V],
    withConsumer: WithShareConsumer[F],
    // What Java reported of the sent acknowledgements since they were last read.
    results: Ref[F, Results],
    // Held while acknowledgements go from the state to Java, so that nothing else uses Java between.
    mutex: Mutex[F],
    state: SignallingRef[F, ShareConsumerState],
    lifecycle: SignallingRef[F, Lifecycle],
    // The driver's last failure, which fails the running `consume`.
    failure: SignallingRef[F, Option[Throwable]]
  )(implicit F: Async[F])
    extends KafkaShareConsumer[F, K, V]
    with Acknowledger[F] {

    override val settings: ShareConsumerSettings[F, K, V] = config.settings

    private val pauseSignal: Signal[F, Boolean] =
      settings.pauseWhen.getOrElse(Signal.constant[F, Boolean](false))

    override def metrics: F[Map[MetricName, Metric]] =
      withConsumer.blocking(_.metrics().asScala.toMap)

    // --- Subscription ---

    override def subscribe[G[_]: Reducible](topics: G[String]): F[Unit] =
      withConsumer.blocking(_.subscribe(topics.toList.asJava))

    // Java's unsubscribe doesn't send what is pending, so it is sent first; a timeout doesn't stop
    // leaving. In implicit mode a commit would accept records still being processed.
    override def unsubscribe: F[Unit] =
      if (config.implicitMode) withConsumer.blocking(_.unsubscribe())
      else
        javaStep(_.takeUnsent) { (consumer, acks) =>
          recordIn(consumer, acks)
          Either
            .catchOnly[TimeoutException](consumer.commitSync(settings.commitTimeout.toJava))
            .void
            .getOrElse(())
          consumer.unsubscribe()
        }

    override def subscription: F[SortedSet[String]] =
      withConsumer.blocking(_.subscription().toSortedSet)

    // A poisoned consumer stays poisoned: that is why it refuses to consume.
    override def stopConsuming: F[Unit] =
      lifecycle.update {
        case Poisoned => Poisoned
        case _ => Stopped
      }

    // Takes something from the state and gives it to Java, under the mutex and uncancelable, so that
    // nothing is lost between them.
    private def javaStep[A, B](
      take: ShareConsumerState => (ShareConsumerState, A)
    )(give: (KafkaByteShareConsumer, A) => B): F[B] =
      mutex.lock.surround(state.modify(take).flatMap(a => withConsumer.blocking(give(_, a)))).uncancelable

    // --- The loop ---

    override def consume(process: Chunk[AcknowledgeableConsumerRecord[F, K, V]] => F[Unit]): F[Unit] =
      if (config.implicitMode) F.raiseError(ImplicitAcknowledgementException())
      else exclusively(loop(processed(process)))

    override def autoConsume(process: Chunk[ConsumerRecord[K, V]] => F[Unit]): F[Unit] =
      if (!config.implicitMode)
        F.raiseError(new IllegalStateException("autoConsume needs share.acknowledgement.mode=implicit"))
      else exclusively(loop(autoProcessed(process)))

    // Uncancelable from taking the consumer until giving it back is in place, or a cancel in between
    // would leave it "already consuming" for good. The driver's failure fails it.
    private def exclusively(run: F[Unit]): F[Unit] =
      F.uncancelable { cancelable =>
        lifecycle.modify {
          case Idle => Consuming -> F.unit
          case other => other -> F.raiseError[Unit](new IllegalStateException(refusal(other)))
        }.flatten *>
          cancelable(
            withConsumer
              .blocking(_.subscription().isEmpty)
              .ifM(
                F.raiseError(NotSubscribedException()),
                failure.set(None) *> F.race(driverFailed, run).void
              )
          ).guarantee(lifecycle.update {
            case Consuming => Idle
            case other => other
          })
      }

    private def refusal(lifecycle: Lifecycle): String =
      lifecycle match {
        case Stopped => "the consumer was stopped"
        case Poisoned => "process failed in implicit mode: close the consumer, which releases its records"
        case _ => "the consumer is already consuming"
      }

    private def stopped: F[Unit] = lifecycle.waitUntil(_ == Stopped)

    private def driverFailed: F[Unit] =
      failure.discrete
        .collectFirst { case Some(error) => error }
        .compile
        .lastOrError
        .flatMap(F.raiseError[Unit])

    private def paused: F[Boolean] = pauseSignal.get

    // Polls until stopped, and gives each poll to `run`. While paused, nothing is polled, and a commit
    // sends what is left, as in implicit mode the poll just processed. A poll waits for the records
    // being renewed to be back, as Java's poll would fetch while a renewal is pending. It can't be
    // canceled until `run` has taken its records: Java holds them from the poll on. `run` lets the
    // processing be canceled again with the `Poll` it gets.
    private def loop(run: (Poll[F], Polled) => F[Unit]): F[Unit] = {
      lazy val next: F[Unit] =
        lifecycle.get.flatMap {
          case Stopped => finish
          case _ =>
            paused.ifM(
              withConsumer.blocking(_.commitAsync()) *> F.race(stopped, pauseSignal.waitUntil(!_)) *> next,
              F.race(stopped, state.waitUntil(!_.hasRenewing)).flatMap(_.fold(_ => next, _ => polling))
            )
        }
      lazy val polling: F[Unit] =
        F.uncancelable { cancelable =>
          F.monotonic.flatMap { start =>
            poll.flatMap {
              case None => F.pure(false)
              case Some((records, lock)) =>
                run(cancelable, Polled(records, start, lock)).whenA(records.nonEmpty).as(true)
            }
          }
        }.flatMap(polled => F.sleep(subscriptionCheckInterval).unlessA(polled) *> next)
      next
    }

    // The poll sends what is left of the last one. Unsubscribed, nothing is polled; Java would throw.
    // What the commit callback reported is of no use with nothing held, so it is dropped.
    private def poll: F[Option[(Vector[KafkaByteConsumerRecord], Option[FiniteDuration])]] =
      results.set(Vector.empty) *> javaStep(_.takeUnsent) { (consumer, acks) =>
        recordIn(consumer, acks)
        if (consumer.subscription().isEmpty) None else Some(pollRecords(consumer, settings.pollTimeout))
      }

    // Stopping waits for the records being renewed, up to the commit timeout, and sends what is left
    // with a commit which waits for the broker, raising its rejection, unless unsubscribed: the
    // records went with the subscription.
    private def finish: F[Unit] =
      state.waitUntil(!_.hasRenewing).timeoutTo(settings.commitTimeout, F.unit) *>
        javaStep(_.takeUnsent) { (consumer, acks) =>
          recordIn(consumer, acks)
          val results = consumer.commitSync(settings.commitTimeout.toJava)
          if (consumer.subscription().isEmpty) None else results.values.asScala.flatMap(_.toScala).headOption
        }.adaptError { case error: TimeoutException =>
          ShareCommitTimeoutException(settings.commitTimeout, error)
        }.flatMap(_.traverse_(F.raiseError[Unit](_)))

    // --- One poll ---

    // In explicit mode the state holds the poll from the moment the consumer takes it, also when
    // decoding fails or `process` is canceled. Once `process` returns, the poll is closed; if it
    // failed or was canceled, no poll follows, so what it left is sent at once.
    private def processed(
      process: Chunk[AcknowledgeableConsumerRecord[F, K, V]] => F[Unit]
    )(cancelable: Poll[F], polled: Polled): F[Unit] =
      F.monotonic.flatMap { now =>
        val deliveries = polled.records.map(r => offsetOf(r) -> Delivery(r, deliveryCountOf(r), now))
        state.update(ShareConsumerState.polled(deliveries, polled.start, polled.lock, _)) *>
          cancelable(Chunk.from(polled.records).traverse(decode).flatMap(process))
            .guaranteeCase(outcome =>
              F.monotonic.flatMap(at => state.update(_.closed(at))) *> send(identity).unlessA(
                outcome.isSuccess
              )
            )
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

    // In implicit mode Java accepts the poll with the next poll or commit. A failure, also a cancel as
    // another of several consumers failed, poisons the consumer.
    private def autoProcessed(
      process: Chunk[ConsumerRecord[K, V]] => F[Unit]
    )(cancelable: Poll[F], polled: Polled): F[Unit] =
      cancelable(
        Chunk
          .from(polled.records)
          .traverse(ConsumerRecord.fromJava(_, keyDeserializer, valueDeserializer))
          .flatMap(process)
      ).guaranteeCase(outcome => lifecycle.set(Poisoned).unlessA(outcome.isSuccess))

    // --- Acknowledgements ---

    // Recording an acknowledgement, a renewal included, is one state update, which touches neither
    // Java nor a lock: the state mirrors what Java holds, so whatever Java would refuse fails here.
    override def acknowledge(acks: Chunk[Acknowledgement]): F[Unit] =
      F.monotonic
        .flatMap(at => state.modify(_.acknowledged(acks, at)))
        .flatMap(_.headOption.traverse_(p => F.raiseError[Unit](RecordNotInFlightException(p._1, p._2))))

    // --- The driver ---

    /**
      * Runs as long as the consumer and does what is due: releases records past the max processing
      * time, renews, and sends. It wakes at the earliest deadline, recomputed whenever the state or
      * the pause changes it; what it does runs apart from that, so that a new deadline can't stop it
      * half-way. A failure fails the running `consume`, and the driver goes on.
      */
    def driver: F[Unit] =
      Signal
        .applicativeInstance[F]
        .map2(state, pauseSignal)((s, p) => actions(s, p).map(_._1).reduceOption(_ min _))
        .discrete
        .changes(cats.Eq.fromUniversalEquals[Option[FiniteDuration]])
        .switchMap {
          case None => Stream.empty
          case Some(at) =>
            Stream.eval(F.monotonic).flatMap(now => Stream.sleep_[F](at - now)) ++ Stream.emit(())
        }
        .evalMap(_ => due.handleErrorWith(error => failure.set(Some(error))))
        .compile
        .drain

    // What the driver does, each with its deadline, in the order they go when due together.
    private def actions(s: ShareConsumerState, paused: Boolean): List[(FiniteDuration, F[Unit])] =
      List(
        settings.maxProcessingTime.flatMap(max => s.nextOverdue(max).tupleRight(releaseOverdue(max))),
        s.nextRenewal(Some(config.renewalInterval(s.lock)).filter(_ => settings.autoRenewal))
          .tupleRight(renew),
        s.nextSend(settings.maxAcknowledgementDelay, paused).tupleRight(send(identity))
      ).flatten

    // Does one due thing at a time and looks again, as what is done moves only its own deadline.
    private def due: F[Unit] =
      (state.get, paused, F.monotonic)
        .flatMapN { (s, p, now) =>
          actions(s, p)
            .collectFirst { case (at, action) if at <= now => action.as(true) }
            .getOrElse(F.pure(false))
        }
        .iterateWhile(identity)
        .void

    private def send(prepare: ShareConsumerState => ShareConsumerState): F[Unit] =
      javaStep(prepare(_).takeUnsent) { (consumer, acks) =>
        recordIn(consumer, acks)
        consumer.commitAsync()
      }

    // Overdue records are released and sent at once, so that a late acknowledgement fails.
    private def releaseOverdue(max: FiniteDuration): F[Unit] =
      F.monotonic.flatMap(now => send(_.withOverdueReleased(max, now)))

    /**
      * Renews every unacknowledged record, sending what is pending with it, without waiting for the
      * broker, as Java's own renewal loop does, then asks Java until the broker answered, and takes
      * the renewed records back with a poll, which fetches nothing as no renewal is pending anymore.
      * Meanwhile there is nothing else to send: every unacknowledged record is being renewed, and an
      * acknowledgement made meanwhile waits for its record. Unsubscribed, nothing is renewed, as
      * Java's poll would refuse to take the records back.
      */
    private def renew: F[Unit] =
      F.monotonic.flatMap { at =>
        withConsumer
          .blocking(_.subscription().isEmpty)
          .ifM(
            state.update(_.withRenewalSkipped(at)),
            results.set(Vector.empty) *>
              javaStep { s =>
                val (taken, acks) = s.takeUnsent
                val (renewing, renewed) = taken.renewal(at)
                (renewing, (acks, renewed))
              } { case (consumer, (acks, renewed)) =>
                recordIn(consumer, acks)
                val sent = renewed.collect { case (p, record) if renewedIn(consumer, record) => p }
                consumer.commitAsync()
                (sent.toSet, acquisitionLock(consumer))
              }.flatMap { case (sent, lock) =>
                val forgetAt = at + lock.getOrElse(Duration.Zero) + config.apiTimeout
                confirmed(sent, forgetAt, Set.empty).flatMap(takeBack)
              }
                // Java failed before it had every renewal: none is counted on.
                .onError { case _ =>
                  F.monotonic.flatMap(now => state.update(_.withRenewalEnded(Vector.empty, None, now)._1))
                }
          )
      }

    // Asks Java, with a commit, until the broker answered for every record sent for renewal, and
    // returns those it confirmed. If it can't answer anymore, or the consumer unsubscribed, none can
    // be taken back: Java's poll would fetch while a renewal is pending, or refuse.
    private def confirmed(
      waiting: Set[TopicPartitionOffset],
      forgetAt: FiniteDuration,
      done: Set[TopicPartitionOffset]
    ): F[Set[TopicPartitionOffset]] =
      F.monotonic.flatMap { now =>
        if (waiting.isEmpty) F.pure(done)
        else if (now >= forgetAt) F.pure(Set.empty[TopicPartitionOffset])
        else
          F.sleep(renewalCheckInterval) *>
            mutex.lock
              .surround(withConsumer.blocking { consumer =>
                consumer.commitAsync()
                consumer.subscription().isEmpty
              })
              .product(results.getAndSet(Vector.empty))
              .flatMap { case (unsubscribed, reported) =>
                val answered = reported.filter { case (p, _) => waiting(p) }
                if (unsubscribed) F.pure(Set.empty[TopicPartitionOffset])
                else
                  confirmed(
                    waiting -- answered.map(_._1),
                    forgetAt,
                    done ++ answered.collect { case (p, true) => p }
                  )
              }
      }

    // A poll takes the confirmed records back, after Java got the held records' acknowledgements,
    // which its poll needs first; the others are lost. Records it returned which no one knows are
    // released.
    private def takeBack(confirmed: Set[TopicPartitionOffset]): F[Unit] =
      javaStep(_.takeUnsent) { (consumer, acks) =>
        recordIn(consumer, acks)
        if (confirmed.isEmpty) (Vector.empty[KafkaByteConsumerRecord], None)
        else pollRecords(consumer, Duration.Zero)
      }.product(F.monotonic).flatMap { case ((returned, lock), at) =>
        state.modify(_.withRenewalEnded(returned, lock, at)).flatMap { unknown =>
          withConsumer
            .blocking { consumer =>
              recordIn(consumer, unknown.map(_ -> AcknowledgeType.Release))
              consumer.commitAsync()
            }
            .whenA(unknown.nonEmpty)
        }
      }

  }

}
