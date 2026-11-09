/*
 * Copyright 2018 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka

import cats.Reducible
import cats.effect.Async
import cats.effect.Outcome
import cats.effect.Poll
import cats.effect.Ref
import cats.effect.Resource
import cats.effect.std.Dispatcher
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
import fs2.kafka.internal.ShareConsumerCore
import fs2.kafka.internal.ShareConsumerScheduler
import fs2.kafka.internal.ShareConsumerState.offsetOf
import fs2.kafka.internal.WithShareConsumer
import fs2.kafka.internal.converters.option.*
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
  * consumer, and a background scheduler sends them when they are due, renews acquisition locks, and
  * acknowledges records past the max processing time with the failure acknowledgement. Anything
  * still pending is sent with the next poll.<br><br>
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
    * the next poll. Records left unacknowledged when `process` returns are acknowledged with
    * `failureAcknowledgement`, `Release` by default. If `process` fails, its records are
    * acknowledged the same way and the error is raised. If it is canceled, they are released.
    * Acknowledging a record after `process` has returned fails with a
    * [[RecordNotInFlightException]].<br><br>
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
    * closing the Java consumer can still run the callback, and the scheduler is stopped before the
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
      core <- Resource.eval(ShareConsumerCore.of(withConsumer, results))
      scheduler <- Resource.eval(ShareConsumerScheduler.of(config, core, pauseOf(settings)))
      lifecycle <- Resource.eval(SignallingRef.of[F, Lifecycle](Idle))
      consumer = new Impl(config, keyDeserializer, valueDeserializer, core, scheduler, lifecycle)
      _ <- scheduler.run.background
    } yield consumer

  /**
    * Creates a new [[KafkaShareConsumer]] in the `Stream` context, using the specified
    * [[ShareConsumerSettings]].
    */
  def stream[F[_], K, V](settings: ShareConsumerSettings[F, K, V])(implicit
    F: Async[F],
    mk: MkShareConsumer[F]
  ): Stream[F, KafkaShareConsumer[F, K, V]] = Stream.resource(resource(settings))

  private def pauseOf[F[_], K, V](settings: ShareConsumerSettings[F, K, V])(implicit
    F: Async[F]
  ): Signal[F, Boolean] =
    settings.pauseWhen.getOrElse(Signal.constant[F, Boolean](false))

  private def deliveryCountOf(record: KafkaByteConsumerRecord): Short =
    record.deliveryCount().toScala.fold(1: Short)(_.shortValue)

  /**
    * How often an unsubscribed consumer checks whether it has been subscribed.
    */
  private val subscriptionCheckInterval: FiniteDuration = 50.millis

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

  final private class Impl[F[_], K, V](
    config: ShareConsumerConfig[F, K, V],
    keyDeserializer: KeyDeserializer[F, K],
    valueDeserializer: ValueDeserializer[F, V],
    core: ShareConsumerCore[F],
    scheduler: ShareConsumerScheduler[F, K, V],
    lifecycle: SignallingRef[F, Lifecycle]
  )(implicit F: Async[F])
    extends KafkaShareConsumer[F, K, V]
    with Acknowledger[F] {

    override val settings: ShareConsumerSettings[F, K, V] = config.settings

    private val pauseSignal: Signal[F, Boolean] = pauseOf(settings)

    override def metrics: F[Map[MetricName, Metric]] =
      core.metrics

    override def subscribe[G[_]: Reducible](topics: G[String]): F[Unit] =
      core.subscribe(topics.toList)

    /**
      * Java's `unsubscribe` does not send pending acknowledgements, so in explicit mode they are
      * committed first, and a commit timeout does not stop the consumer from unsubscribing. In
      * implicit mode, nothing is committed, as a commit would accept records still being processed.
      */
    override def unsubscribe: F[Unit] =
      if (config.implicitMode) core.unsubscribe
      else
        core.commitAndUnsubscribe(settings.commitTimeout)

    override def subscription: F[SortedSet[String]] =
      core.subscription

    /**
      * A poisoned consumer stays poisoned, so that the reason it refuses to consume is kept.
      */
    override def stopConsuming: F[Unit] =
      lifecycle.update {
        case Poisoned => Poisoned
        case _ => Stopped
      }

    override def consume(process: Chunk[AcknowledgeableConsumerRecord[F, K, V]] => F[Unit]): F[Unit] =
      if (config.implicitMode) F.raiseError(ImplicitAcknowledgementException())
      else exclusively(loop(processed(process)))

    override def autoConsume(process: Chunk[ConsumerRecord[K, V]] => F[Unit]): F[Unit] =
      if (!config.implicitMode)
        F.raiseError(new IllegalStateException("autoConsume needs share.acknowledgement.mode=implicit"))
      else exclusively(loop(autoProcessed(process)))

    /**
      * Runs `run` as the only consumption of the consumer, and fails if the scheduler fails.<br><br>
      *
      * It is uncancelable from `startConsuming` until `endConsuming` is in place as its finalizer,
      * as a cancelation in between would leave the consumer consuming for good.
      */
    private def exclusively(run: F[Unit]): F[Unit] =
      F.uncancelable { cancelable =>
        startConsuming *>
          cancelable(requireSubscribed *> scheduler.reset *> F.race(scheduler.failed, run).void)
            .guarantee(endConsuming)
      }

    /**
      * Marks the consumer as consuming, or fails with the reason it can't consume.
      */
    private def startConsuming: F[Unit] =
      lifecycle.modify {
        case Idle => Consuming -> F.unit
        case other => other -> F.raiseError[Unit](new IllegalStateException(refusal(other)))
      }.flatten

    private def requireSubscribed: F[Unit] =
      core.subscribed.ifM(F.unit, F.raiseError(NotSubscribedException()))

    /**
      * Sends the pending acknowledgements, as no poll follows to send them, and marks the consumer
      * as no longer consuming, unless it was stopped or poisoned meanwhile.
      */
    private def endConsuming: F[Unit] =
      core.send.attempt.void *> lifecycle.update {
        case Consuming => Idle
        case other => other
      }

    private def refusal(lifecycle: Lifecycle): String =
      lifecycle match {
        case Stopped => "the consumer was stopped"
        case Poisoned => "process failed in implicit mode: close the consumer, which releases its records"
        case _ => "the consumer is already consuming"
      }

    /**
      * Polls until stopped, and passes the records of each poll to `run`. Stopping finishes with
      * `finish`. While paused, or while records are being renewed, the loop waits instead of
      * polling.
      */
    private def loop(run: (Poll[F], Polled) => F[Unit]): F[Unit] =
      (lifecycle.get, pauseSignal.get, core.renewing).flatMapN { (current, paused, renewing) =>
        if (current == Stopped) finish
        else {
          val step =
            if (paused) waitForUnpause
            else if (renewing) waitForRenewals
            else pollOnce(run)
          step >> loop(run)
        }
      }

    /**
      * Waits until the pause ends, or the consumer is stopped. Nothing is polled meanwhile, so a
      * commit sends the pending acknowledgements, which in implicit mode accepts the poll just
      * processed.
      */
    private def waitForUnpause: F[Unit] =
      core.commit *> untilStopped(pauseSignal.waitUntil(!_))

    /**
      * Waits until the records being renewed are back, or the consumer is stopped, as Java's poll
      * would fetch more records while a renewal is pending.
      */
    private def waitForRenewals: F[Unit] =
      untilStopped(core.awaitRenewals)

    private def untilStopped(wait: F[Unit]): F[Unit] =
      F.race(lifecycle.waitUntil(_ == Stopped), wait).void

    /**
      * Polls once and passes the records to `run`, or waits a little if unsubscribed. The poll
      * can't be canceled until `run` has taken its records, as Java holds them from the poll
      * onwards. `run` makes processing cancelable again with the `Poll` it is given.<br><br>
      *
      * The commit callback's reports are dropped before polling.
      */
    private def pollOnce(run: (Poll[F], Polled) => F[Unit]): F[Unit] =
      F.uncancelable { cancelable =>
        for {
          start <- F.monotonic
          polled <- core.poll(settings.pollTimeout)
          _ <- polled.traverse_ { case (records, lock) =>
            run(cancelable, Polled(records, start, lock)).whenA(records.nonEmpty)
          }
        } yield polled.isDefined
      }.flatMap(subscribed => F.sleep(subscriptionCheckInterval).unlessA(subscribed))

    /**
      * Waits for the records being renewed, up to the commit timeout, and sends the pending
      * acknowledgements with a synchronous commit. Raises the first acknowledgement the broker
      * rejected.
      */
    private def finish: F[Unit] =
      for {
        _ <- core.awaitRenewals.timeoutTo(settings.commitTimeout, F.unit)
        rejected <- core.commitSync(settings.commitTimeout).adaptError { case error: TimeoutException =>
          ShareCommitTimeoutException(settings.commitTimeout, error)
        }
        _ <- rejected.traverse_(F.raiseError[Unit](_))
      } yield ()

    /**
      * Processes a poll in explicit mode. The state holds the records from the moment they are
      * taken, also when decoding fails or `process` is canceled. Once `process` returns, the poll
      * is closed. If `process` failed or was canceled, `consume` ends, which sends the pending
      * acknowledgements.
      */
    private def processed(
      process: Chunk[AcknowledgeableConsumerRecord[F, K, V]] => F[Unit]
    )(cancelable: Poll[F], polled: Polled): F[Unit] =
      hold(polled) *>
        cancelable(Chunk.from(polled.records).traverse(decodeAcknowledgeable).flatMap(process))
          .guaranteeCase(closePoll)

    /**
      * Processes a poll in implicit mode, where Java accepts the records with the next poll or
      * commit. A failure poisons the consumer, as does a cancelation, for example when another of
      * several consumers fails.
      */
    private def autoProcessed(
      process: Chunk[ConsumerRecord[K, V]] => F[Unit]
    )(cancelable: Poll[F], polled: Polled): F[Unit] =
      cancelable(Chunk.from(polled.records).traverse(decode).flatMap(process))
        .guaranteeCase(outcome => lifecycle.set(Poisoned).unlessA(outcome.isSuccess))

    private def hold(polled: Polled): F[Unit] =
      F.monotonic.flatMap { now =>
        val deliveries = polled.records.map(r => offsetOf(r) -> Delivery(r, deliveryCountOf(r), now))
        core.hold(deliveries, polled.start, polled.lock)
      }

    /**
      * Closes the poll once `process` is done: the records it left are acknowledged with
      * `failureAcknowledgement`, or released if it was canceled, as they were only interrupted.
      */
    private def closePoll(outcome: Outcome[F, Throwable, Unit]): F[Unit] = {
      val kind = if (outcome.isCanceled) AcknowledgeType.Release else settings.failureAcknowledgement
      core.close(kind)
    }

    private def decode(record: KafkaByteConsumerRecord): F[ConsumerRecord[K, V]] =
      ConsumerRecord.fromJava(record, keyDeserializer, valueDeserializer)

    private def decodeAcknowledgeable(
      record: KafkaByteConsumerRecord
    ): F[AcknowledgeableConsumerRecord[F, K, V]] =
      decode(record).map { decoded =>
        val (partition, offset) = offsetOf(record)
        AcknowledgeableConsumerRecord(
          decoded,
          AcknowledgeableOffset(partition, offset, deliveryCountOf(record), this)
        )
      }

    /**
      * Records the acknowledgements, renewals included, with a single state update, without
      * touching the Java consumer or the mutex. The state mirrors what Java holds, so anything Java
      * would refuse fails here with a [[RecordNotInFlightException]].
      */
    override def acknowledge(acks: Chunk[Acknowledgement]): F[Unit] =
      core
        .acknowledge(acks)
        .flatMap(_.headOption.traverse_(p => F.raiseError[Unit](RecordNotInFlightException(p._1, p._2))))

  }

}
