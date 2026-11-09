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
  * [[KafkaShareConsumer]] represents a consumer of Kafka records in a share group, with the
  * ability to `subscribe` to topics and process records with [[consume]] or [[autoConsume]], and
  * stop with [[stopConsuming]].<br><br>
  *
  * Consuming works like Java's poll loop: the consumer polls, runs `process` on the records of the
  * poll, and polls again once `process` has returned. Acknowledgements are recorded by the
  * consumer, and a background driver sends them when they are due, renews acquisition locks, and
  * releases records past the max processing time. Anything still pending is sent with the next
  * poll.<br><br>
  *
  * @note
  *   this API is experimental, as are Kafka's share groups.
  */
sealed abstract class KafkaShareConsumer[F[_], K, V] extends KafkaShareSubscription[F] with KafkaMetrics[F] {

  /**
    * Returns the settings used to create the consumer instance.
    */
  def settings: ShareConsumerSettings[F, K, V]

  /**
    * Consumes records in explicit acknowledgement mode, running `process` on the records of each
    * poll, one poll at a time, until [[stopConsuming]] is invoked.<br><br>
    *
    * `process` may work on the records in parallel and acknowledge each record as it finishes.
    * Acknowledgements are sent after `maxAcknowledgementDelay`, immediately while paused, or with
    * the next poll. Records left unacknowledged when `process` returns are released. If `process`
    * fails, its records are released and the error is raised. Acknowledging a record after
    * `process` has returned fails with a [[RecordNotInFlightException]].<br><br>
    *
    * The last acknowledgements are sent before this returns, and it fails if the broker rejects
    * them. Acknowledgements sent earlier are not awaited, so they can fail unnoticed, in which case
    * their records are delivered again.
    */
  def consume(process: Chunk[AcknowledgeableConsumerRecord[F, K, V]] => F[Unit]): F[Unit]

  /**
    * Consumes records in implicit acknowledgement mode, running `process` on the records of each
    * poll until [[stopConsuming]] is invoked. Once `process` returns, the records of the poll are
    * accepted.<br><br>
    *
    * If `process` fails, the records are not accepted: closing the consumer releases them, so they
    * are delivered again, as in Java's poll loop. Since Java would accept them with its next poll,
    * the consumer then refuses to consume again, and you have to close it and create a new one.
    */
  def autoConsume(process: Chunk[ConsumerRecord[K, V]] => F[Unit]): F[Unit]

  /**
    * Stops consuming records. [[consume]] or [[autoConsume]] returns once the current poll has been
    * processed and the last acknowledgements have been sent.<br><br>
    *
    * Stopping is final: consuming again fails with an `IllegalStateException`, so create a new
    * consumer instead.
    */
  def stopConsuming: F[Unit]

}

object KafkaShareConsumer {

  /**
    * Creates a new [[KafkaShareConsumer]] in the `Resource` context, using the specified
    * [[ShareConsumerSettings]].<br><br>
    *
    * The commit callback is only set in explicit acknowledgement mode, which is the only mode that
    * renews records. The `Dispatcher` running the callback is created before the Java consumer, as
    * closing the Java consumer can still run the callback, and the driver is stopped before the
    * Java consumer is closed.
    */
  def resource[F[_], K, V](
    settings: ShareConsumerSettings[F, K, V]
  )(implicit F: Async[F], mk: MkShareConsumer[F]): Resource[F, KafkaShareConsumer[F, K, V]] =
    for {
      keyDeserializer <- settings.keyDeserializer
      valueDeserializer <- settings.valueDeserializer
      config <- Resource.eval(ShareConsumerConfig.from(settings))
      dispatcher <- Dispatcher.sequential[F]
      withConsumer <- WithShareConsumer(mk, settings)
      results <- Resource.eval(Ref.of[F, Results](Vector.empty))
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
      _ <- consumer.driver.background
    } yield consumer

  /**
    * Creates a new [[KafkaShareConsumer]] in the `Stream` context, using the specified
    * [[ShareConsumerSettings]].
    */
  def stream[F[_], K, V](settings: ShareConsumerSettings[F, K, V])(implicit
    F: Async[F],
    mk: MkShareConsumer[F]
  ): Stream[F, KafkaShareConsumer[F, K, V]] = Stream.resource(resource(settings))

  private def deliveryCountOf(record: KafkaByteConsumerRecord): Short =
    record.deliveryCount().toScala.fold(1: Short)(_.shortValue)

  /**
    * Returns the acquisition lock duration last reported by the broker.
    */
  private def acquisitionLock(consumer: KafkaByteShareConsumer): Option[FiniteDuration] =
    consumer.acquisitionLockTimeoutMs().toScala.map(_.intValue.millis)

  private def pollRecords(
    consumer: KafkaByteShareConsumer,
    timeout: FiniteDuration
  ): (Vector[KafkaByteConsumerRecord], Option[FiniteDuration]) =
    consumer.poll(timeout.toJava).iterator().asScala.toVector -> acquisitionLock(consumer)

  /**
    * Gives every acknowledgement to the Java consumer, and then throws the first error, ignoring
    * the `IllegalStateException` Java throws for a record it no longer holds.<br><br>
    *
    * The acknowledgements have already been taken from the state, so a single failure must not
    * lose the rest.
    */
  private def recordIn(
    consumer: KafkaByteShareConsumer,
    acks: Vector[(KafkaByteConsumerRecord, AcknowledgeType)]
  ): Unit =
    acks
      .map { case (record, kind) => Either.catchNonFatal(consumer.acknowledge(record, kind.asJava)) }
      .collectFirst { case Left(error) if !error.isInstanceOf[IllegalStateException] => error }
      .foreach(error => throw error)

  /**
    * Renews the record with the Java consumer, and returns whether Java accepted the renewal. Java
    * refuses to renew a record it no longer holds.
    */
  private def renewedIn(consumer: KafkaByteShareConsumer, record: KafkaByteConsumerRecord): Boolean =
    Either
      .catchOnly[IllegalStateException](consumer.acknowledge(record, AcknowledgeType.Renew.asJava))
      .isRight

  /**
    * How often an unsubscribed consumer checks whether it has been subscribed.
    */
  private val subscriptionCheckInterval: FiniteDuration = 50.millis

  /**
    * How long the driver waits before retrying an action which failed.
    */
  private val retryInterval: FiniteDuration = 1.second

  /**
    * How often a renewal checks with the Java consumer whether the broker has answered.
    */
  private val renewalCheckInterval: FiniteDuration = 10.millis

  /**
    * The records of a poll, when the poll started, and the acquisition lock duration reported by
    * the broker.
    */
  final private case class Polled(
    records: Vector[KafkaByteConsumerRecord],
    start: FiniteDuration,
    lock: Option[FiniteDuration]
  )

  /**
    * Whether the consumer is able to consume. Stopping is final. In implicit mode, the consumer is
    * poisoned once `process` has failed, since any later poll or commit would accept its records.
    */
  sealed abstract private class Lifecycle
  private case object Idle extends Lifecycle
  private case object Consuming extends Lifecycle
  private case object Stopped extends Lifecycle
  private case object Poisoned extends Lifecycle

  /**
    * @param results
    *   what the commit callback reported for sent acknowledgements since they were last read
    * @param mutex
    *   held while acknowledgements are moved from the state to the Java consumer, so that nothing
    *   else uses the Java consumer in between
    * @param failure
    *   the last failure of the driver, which fails the running `consume`
    */
  final private class Impl[F[_], K, V](
    config: ShareConsumerConfig[F, K, V],
    keyDeserializer: KeyDeserializer[F, K],
    valueDeserializer: ValueDeserializer[F, V],
    withConsumer: WithShareConsumer[F],
    results: Ref[F, Results],
    mutex: Mutex[F],
    state: SignallingRef[F, ShareConsumerState],
    lifecycle: SignallingRef[F, Lifecycle],
    failure: SignallingRef[F, Option[Throwable]]
  )(implicit F: Async[F])
    extends KafkaShareConsumer[F, K, V]
    with Acknowledger[F] {

    override val settings: ShareConsumerSettings[F, K, V] = config.settings

    private val pauseSignal: Signal[F, Boolean] =
      settings.pauseWhen.getOrElse(Signal.constant[F, Boolean](false))

    override def metrics: F[Map[MetricName, Metric]] =
      withConsumer.blocking(_.metrics().asScala.toMap)

    override def subscribe[G[_]: Reducible](topics: G[String]): F[Unit] =
      withConsumer.blocking(_.subscribe(topics.toList.asJava))

    /**
      * Java's `unsubscribe` does not send pending acknowledgements, so in explicit mode they are
      * committed first, and a commit timeout does not stop the consumer from unsubscribing. In
      * implicit mode, nothing is committed, as a commit would accept records still being processed.
      */
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

    /**
      * A poisoned consumer stays poisoned, so that the reason it refuses to consume is kept.
      */
    override def stopConsuming: F[Unit] =
      lifecycle.update {
        case Poisoned => Poisoned
        case _ => Stopped
      }

    /**
      * Takes something from the state with `take`, and gives it to the Java consumer with `give`.
      * Runs holding the mutex and uncancelable, so that nothing is lost in between.
      */
    private def javaStep[A, B](
      take: ShareConsumerState => (ShareConsumerState, A)
    )(give: (KafkaByteShareConsumer, A) => B): F[B] =
      mutex.lock.surround(state.modify(take).flatMap(a => withConsumer.blocking(give(_, a)))).uncancelable

    override def consume(process: Chunk[AcknowledgeableConsumerRecord[F, K, V]] => F[Unit]): F[Unit] =
      if (config.implicitMode) F.raiseError(ImplicitAcknowledgementException())
      else exclusively(loop(processed(process)))

    override def autoConsume(process: Chunk[ConsumerRecord[K, V]] => F[Unit]): F[Unit] =
      if (!config.implicitMode)
        F.raiseError(new IllegalStateException("autoConsume needs share.acknowledgement.mode=implicit"))
      else exclusively(loop(autoProcessed(process)))

    /**
      * Runs `run` as the only consumption of the consumer, and fails if the driver fails.<br><br>
      *
      * It is uncancelable from marking the consumer as consuming until the finalizer which unmarks
      * it is in place, as a cancelation in between would leave the consumer consuming for good.
      * However consumption ends, pending acknowledgements are sent, as no poll follows to send
      * them.
      */
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
          ).guarantee(
            send(identity).attempt.void *> lifecycle.update {
              case Consuming => Idle
              case other => other
            }
          )
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

    /**
      * Polls until stopped, and passes the records of each poll to `run`.<br><br>
      *
      * The loop works as follows.
      *   - While paused, nothing is polled, and a commit sends pending acknowledgements, which in
      *     implicit mode accepts the poll just processed.
      *   - While records are being renewed, polling waits until they are back, as Java's poll would
      *     fetch more records while a renewal is pending.
      *   - A poll can't be canceled until `run` has taken its records, as Java holds them from the
      *     poll onwards. `run` makes processing cancelable again with the `Poll` it is given.
      */
    private def loop(run: (Poll[F], Polled) => F[Unit]): F[Unit] = {
      lazy val next: F[Unit] =
        lifecycle.get.flatMap {
          case Stopped => finish
          case _ =>
            (paused, state.get.map(_.hasRenewing)).tupled.flatMap {
              case (true, _) =>
                withConsumer.blocking(_.commitAsync()) *> F.race(stopped, pauseSignal.waitUntil(!_)) *> next
              case (_, true) => F.race(stopped, state.waitUntil(!_.hasRenewing)) *> next
              case _ => polling
            }
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

    /**
      * Polls the Java consumer, which also sends the pending acknowledgements of the last poll.
      * Returns `None` without polling if unsubscribed, as Java's poll would throw.<br><br>
      *
      * What the commit callback has reported is of no use with no records held, so it is dropped.
      */
    private def poll: F[Option[(Vector[KafkaByteConsumerRecord], Option[FiniteDuration])]] =
      results.set(Vector.empty) *> javaStep(_.takeUnsent) { (consumer, acks) =>
        recordIn(consumer, acks)
        if (consumer.subscription().isEmpty) None else Some(pollRecords(consumer, settings.pollTimeout))
      }

    /**
      * Waits for the records being renewed, up to the commit timeout, and sends pending
      * acknowledgements with a synchronous commit. Raises the first acknowledgement the broker
      * rejected, unless unsubscribed, as the records were released along with the subscription.
      */
    private def finish: F[Unit] =
      state.waitUntil(!_.hasRenewing).timeoutTo(settings.commitTimeout, F.unit) *>
        javaStep(_.takeUnsent) { (consumer, acks) =>
          recordIn(consumer, acks)
          val results = consumer.commitSync(settings.commitTimeout.toJava)
          if (consumer.subscription().isEmpty) None else results.values.asScala.flatMap(_.toScala).headOption
        }.adaptError { case error: TimeoutException =>
          ShareCommitTimeoutException(settings.commitTimeout, error)
        }.flatMap(_.traverse_(F.raiseError[Unit](_)))

    /**
      * Processes a poll in explicit mode. The state holds the records from the moment they are
      * taken, also when decoding fails or `process` is canceled. Once `process` returns, the poll
      * is closed. If `process` failed or was canceled, `consume` ends, which sends the pending
      * acknowledgements.
      */
    private def processed(
      process: Chunk[AcknowledgeableConsumerRecord[F, K, V]] => F[Unit]
    )(cancelable: Poll[F], polled: Polled): F[Unit] =
      F.monotonic.flatMap { now =>
        val deliveries = polled.records.map(r => offsetOf(r) -> Delivery(r, deliveryCountOf(r), now))
        state.update(s => ShareConsumerState.polled(deliveries, polled.start, polled.lock.orElse(s.lock))) *>
          cancelable(Chunk.from(polled.records).traverse(decode).flatMap(process))
            .guarantee(F.monotonic.flatMap(at => state.update(_.closed(at))))
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

    /**
      * Processes a poll in implicit mode, where Java accepts the records with the next poll or
      * commit. A failure poisons the consumer, as does a cancelation, for example when another of
      * several consumers fails.
      */
    private def autoProcessed(
      process: Chunk[ConsumerRecord[K, V]] => F[Unit]
    )(cancelable: Poll[F], polled: Polled): F[Unit] =
      cancelable(
        Chunk
          .from(polled.records)
          .traverse(ConsumerRecord.fromJava(_, keyDeserializer, valueDeserializer))
          .flatMap(process)
      ).guaranteeCase(outcome => lifecycle.set(Poisoned).unlessA(outcome.isSuccess))

    /**
      * Records the acknowledgements, renewals included, with a single state update, without
      * touching the Java consumer or the mutex. The state mirrors what Java holds, so anything Java
      * would refuse fails here with a [[RecordNotInFlightException]].
      */
    override def acknowledge(acks: Chunk[Acknowledgement]): F[Unit] =
      F.monotonic
        .flatMap(at => state.modify(_.acknowledged(acks, at)))
        .flatMap(_.headOption.traverse_(p => F.raiseError[Unit](RecordNotInFlightException(p._1, p._2))))

    /**
      * Runs for as long as the consumer, releasing records past the max processing time, renewing
      * records, and sending acknowledgements when they are due.<br><br>
      *
      * It wakes up at the earliest deadline, which is recomputed whenever the state or the pause
      * signal changes. Due actions run outside of that, so that a new deadline can't interrupt
      * them. A failure fails the running `consume`, and the driver keeps running.
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
        .evalMap(_ => retried(due))
        .compile
        .drain

    /**
      * Runs `step`, retrying it after a failure, as the deadline which made it due might not
      * change. Each failure fails the running `consume`.
      */
    private def retried(step: F[Unit]): F[Unit] =
      step.handleErrorWith(error => failure.set(Some(error)) *> F.sleep(retryInterval) *> retried(step))

    /**
      * Returns the actions of the driver with their deadlines, in the order they run when due at
      * the same time.
      */
    private def actions(s: ShareConsumerState, paused: Boolean): List[(FiniteDuration, F[Unit])] =
      List(
        settings.maxProcessingTime.flatMap(max => s.nextOverdue(max).tupleRight(releaseOverdue(max))),
        s.nextRenewal(autoRenewalInterval(s)).tupleRight(renew),
        s.nextSend(settings.maxAcknowledgementDelay, paused).tupleRight(send(identity))
      ).flatten

    private def autoRenewalInterval(s: ShareConsumerState): Option[FiniteDuration] =
      Some(config.renewalInterval(s.lock)).filter(_ => settings.autoRenewal)

    /**
      * Runs due actions one at a time until none is due, as an action only moves its own deadline.
      */
    private def due: F[Unit] =
      (state.get, paused, F.monotonic)
        .flatMapN { (s, p, now) =>
          actions(s, p)
            .collectFirst { case (at, action) if at <= now => action.as(true) }
            .getOrElse(F.pure(false))
        }
        .iterateWhile(identity)
        .void

    /**
      * Sends pending acknowledgements with an asynchronous commit, after updating the state with
      * `prepare`. Only commits if there is something to send, as in implicit mode a commit would
      * accept the poll.
      */
    private def send(prepare: ShareConsumerState => ShareConsumerState): F[Unit] =
      javaStep(prepare(_).takeUnsent) { (consumer, acks) =>
        if (acks.nonEmpty) {
          recordIn(consumer, acks)
          consumer.commitAsync()
        }
      }

    /**
      * Releases records past the max processing time and sends the releases immediately, so that a
      * late acknowledgement fails.
      */
    private def releaseOverdue(max: FiniteDuration): F[Unit] =
      F.monotonic.flatMap(now => send(_.withOverdueReleased(max, now)))

    /**
      * Renews every unacknowledged record, along with the pending acknowledgements.<br><br>
      *
      * Renewing works as follows.
      *   1. The renewals are sent without waiting for the broker, as in Java's own renewal loop.
      *      The renewal is skipped if it is no longer due once the mutex is held, as a poll since
      *      then would otherwise have its new records renewed.
      *   2. `confirmed` checks with the Java consumer until the broker has answered.
      *   3. `takeBack` takes the renewed records back with a poll, which fetches nothing, as no
      *      renewal is pending anymore.
      *
      * Meanwhile, there is nothing else to send, as every unacknowledged record is being renewed,
      * and acknowledgements made in the meantime wait for their records. If the Java consumer fails
      * on the way, the records are held again as they were, to be renewed or released later,
      * rather than being left renewing forever. Nothing is renewed while unsubscribed, as Java's
      * poll would refuse to take the records back.
      */
    private def renew: F[Unit] =
      F.monotonic.flatMap { at =>
        withConsumer
          .blocking(_.subscription().isEmpty)
          .ifM(
            state.update(_.withRenewalSkipped(at)),
            results.set(Vector.empty) *>
              javaStep { s =>
                if (s.nextRenewal(autoRenewalInterval(s)).exists(_ <= at)) {
                  val (taken, acks) = s.takeUnsent
                  val (renewing, renewed) = taken.renewal(at)
                  (renewing, Some(acks -> renewed))
                } else (s, None)
              } {
                case (consumer, Some((acks, renewed))) =>
                  recordIn(consumer, acks)
                  val sent = renewed.collect { case (p, record) if renewedIn(consumer, record) => p }
                  consumer.commitAsync()
                  Some(sent.toSet -> acquisitionLock(consumer))
                case (_, None) => None
              }
                .flatMap(_.traverse_ { case (sent, lock) =>
                  val forgetAt = at + lock.getOrElse(Duration.Zero) + config.apiTimeout
                  confirmed(sent, forgetAt, Set.empty).flatMap(takeBack)
                })
                .onError { case _ => F.monotonic.flatMap(now => state.update(_.withRenewalFailed(now))) }
          )
      }

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
          F.sleep(renewalCheckInterval) *>
            mutex.lock
              .surround(withConsumer.blocking { consumer =>
                consumer.commitAsync()
                consumer.subscription().isEmpty
              })
              .handleError(_ => false)
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

    /**
      * Takes the confirmed records back with a poll, after giving the Java consumer the
      * acknowledgements of the records it holds, which its poll requires. Records whose renewal was
      * not confirmed are lost.<br><br>
      *
      * Returned records which are unknown to the state are released. Nothing is polled if the
      * consumer has unsubscribed since the check, as Java's poll would throw, and the records were
      * released along with the subscription. If the poll has closed by the time the records are
      * back, their acknowledgements are sent immediately, as `consume` may have ended.
      */
    private def takeBack(confirmed: Set[TopicPartitionOffset]): F[Unit] =
      javaStep(_.takeUnsent) { (consumer, acks) =>
        recordIn(consumer, acks)
        if (confirmed.isEmpty || consumer.subscription().isEmpty)
          (Vector.empty[KafkaByteConsumerRecord], None)
        else pollRecords(consumer, Duration.Zero)
      }.product(F.monotonic).flatMap { case ((returned, lock), at) =>
        state.modify(_.withRenewalEnded(returned, lock, at)).flatMap { unknown =>
          withConsumer
            .blocking { consumer =>
              recordIn(consumer, unknown.map(_ -> AcknowledgeType.Release))
              consumer.commitAsync()
            }
            .whenA(unknown.nonEmpty) *> state.get.map(_.open).ifM(F.unit, send(identity))
        }
      }

  }

}
