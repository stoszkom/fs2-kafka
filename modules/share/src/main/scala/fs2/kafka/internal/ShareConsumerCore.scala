/*
 * Copyright 2018 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka.internal

import cats.effect.Async
import cats.effect.Ref
import cats.effect.std.Mutex
import cats.effect.syntax.all.*
import cats.syntax.all.*
import fs2.Chunk
import fs2.concurrent.Signal
import fs2.concurrent.SignallingRef
import fs2.kafka.AcknowledgeType
import fs2.kafka.KafkaByteConsumerRecord
import fs2.kafka.KafkaByteShareConsumer
import fs2.kafka.internal.Acknowledgement.TopicPartitionOffset
import fs2.kafka.internal.AcknowledgementResults.Results
import fs2.kafka.internal.ShareConsumerCore.*
import fs2.kafka.internal.ShareConsumerState.Acks
import fs2.kafka.internal.converters.collection.*
import fs2.kafka.internal.converters.option.*
import fs2.kafka.internal.syntax.*
import org.apache.kafka.common.KafkaException
import org.apache.kafka.common.Metric
import org.apache.kafka.common.MetricName
import org.apache.kafka.common.errors.TimeoutException
import scala.collection.immutable.SortedSet
import scala.concurrent.duration.*

/**
  * The Java share consumer, and the records it holds for the share consumer with their
  * acknowledgements. It is the share consumer's only access to both.<br><br>
  *
  * Acknowledgements are taken from the state and given to Java in one step, holding the mutex
  * and uncancelable, so that nothing is lost in between and nothing else uses the Java consumer
  * meanwhile. The state otherwise only changes through the operations here, each running the
  * `ShareConsumerState` method of the same name.
  *
  * @param mutex
  *   held while acknowledgements are moved from the state to the Java consumer
  * @param results
  *   what the commit callback reported for sent acknowledgements since they were last read
  */
final private[kafka] class ShareConsumerCore[F[_]] private (
  withConsumer: WithShareConsumer[F],
  mutex: Mutex[F],
  state: SignallingRef[F, ShareConsumerState],
  results: Ref[F, Results]
)(implicit F: Async[F]) {

  def metrics: F[Map[MetricName, Metric]] =
    withConsumer.blocking(_.metrics().asScala.toMap)

  def subscribe(topics: List[String]): F[Unit] =
    withConsumer.blocking(_.subscribe(topics.asJava))

  def subscription: F[SortedSet[String]] =
    withConsumer.blocking(_.subscription().toSortedSet)

  def subscribed: F[Boolean] =
    withConsumer.blocking(!_.subscription().isEmpty)

  /**
    * Unsubscribes without sending anything first, for implicit mode, where a commit would accept
    * records still being processed.
    */
  def unsubscribe: F[Unit] =
    withConsumer.blocking(_.unsubscribe())

  /**
    * Sends what Java has pending with an asynchronous commit.
    */
  def commit: F[Unit] =
    withConsumer.blocking(_.commitAsync())

  /**
    * The held records, read-only, for the scheduler's deadlines.
    */
  def held: Signal[F, HeldRecords] = state.map(s => s: HeldRecords)

  def open: F[Boolean] = state.get.map(_.open)

  def renewing: F[Boolean] = state.get.map(_.renewing)

  /**
    * Waits until no record is being renewed.
    */
  def awaitRenewals: F[Unit] = state.waitUntil(!_.renewing)

  def hold(
    deliveries: Vector[(TopicPartitionOffset, Delivery)],
    start: FiniteDuration,
    reportedLock: Option[FiniteDuration]
  ): F[Unit] =
    state.update(_.hold(deliveries, start, reportedLock))

  def close(kind: AcknowledgeType): F[Unit] =
    F.monotonic.flatMap(at => state.update(_.close(kind, at)))

  def acknowledge(acks: Chunk[Acknowledgement]): F[Vector[TopicPartitionOffset]] =
    F.monotonic.flatMap(at => state.modify(_.acknowledge(acks, at)))

  def skipRenewal(at: FiniteDuration): F[Unit] =
    state.update(_.skipRenewal(at))

  def failRenewal: F[Unit] =
    F.monotonic.flatMap(at => state.update(_.failRenewal(at)))

  /**
    * Polls for up to `timeout`, which sends the pending acknowledgements. Returns the records and
    * the lock the broker reported, or `None` without polling if unsubscribed, as Java's poll would
    * throw. What the commit callback has reported is of no use with no records held, so it is
    * dropped.
    */
  def poll(timeout: FiniteDuration): F[Option[(Vector[KafkaByteConsumerRecord], Option[FiniteDuration])]] =
    results.set(Vector.empty) *> step(_.takeUnsent) { (consumer, acks) =>
      recordIn(consumer, acks)
      if (consumer.subscription().isEmpty) None else Some(pollRecords(consumer, timeout))
    }

  /**
    * Sends the pending acknowledgements with a synchronous commit, waiting up to `timeout`.
    * Returns the first acknowledgement the broker rejected, unless unsubscribed, as the records
    * were released along with the subscription.
    */
  def commitSync(timeout: FiniteDuration): F[Option[KafkaException]] =
    step(_.takeUnsent) { (consumer, acks) =>
      recordIn(consumer, acks)
      val results = consumer.commitSync(timeout.toJava)
      if (consumer.subscription().isEmpty) None
      else results.values.asScala.flatMap(_.toScala).headOption
    }

  /**
    * Sends the pending acknowledgements with a synchronous commit, waiting up to `timeout`, and
    * unsubscribes, also when the commit times out, as Java's `unsubscribe` doesn't send them.
    */
  def commitAndUnsubscribe(timeout: FiniteDuration): F[Unit] =
    step(_.takeUnsent) { (consumer, acks) =>
      recordIn(consumer, acks)
      Either.catchOnly[TimeoutException](consumer.commitSync(timeout.toJava)).void.getOrElse(())
      consumer.unsubscribe()
    }

  /**
    * Sends the pending acknowledgements with an asynchronous commit.
    */
  def send: F[Unit] =
    sending(_.takeUnsent)

  /**
    * Acknowledges the records held without an acknowledgement for `max` with `kind`, and sends
    * the acknowledgements with an asynchronous commit.
    */
  def sendOverdue(max: FiniteDuration, kind: AcknowledgeType): F[Unit] =
    F.monotonic.flatMap(now => sending(_.takeOverdue(max, kind, now)))

  /**
    * Gives Java the pending acknowledgements and a `Renew` for every unacknowledged record, if the
    * renewal is still `due`, and commits without waiting. Returns the records Java took for
    * renewal, and the lock the broker reported, or `None` if the renewal is no longer due.
    */
  def renew(at: FiniteDuration)(
    due: HeldRecords => Boolean
  ): F[Option[(Set[TopicPartitionOffset], Option[FiniteDuration])]] =
    results.set(Vector.empty) *> step { s =>
      if (due(s)) {
        val (renewing, renewal) = s.takeRenewal(at)
        (renewing, Some(renewal))
      } else (s, None)
    } { (consumer, taken) =>
      taken.map { renewal =>
        recordIn(consumer, renewal.acks)
        val sent = renewal.records.collect { case (p, record) if renewedIn(consumer, record) => p }
        consumer.commitAsync()
        sent.toSet -> acquisitionLock(consumer)
      }
    }

  /**
    * Lets the Java consumer run the commit callback with a commit, holding the mutex, and returns
    * what the callback reported since the last check, or `None` if the consumer is unsubscribed. A
    * failed commit counts as subscribed, as Java still has what was sent.
    */
  def checkRenewals: F[Option[Results]] =
    mutex.lock
      .surround(withConsumer.blocking { consumer =>
        consumer.commitAsync()
        consumer.subscription().isEmpty
      })
      .handleError(_ => false)
      .flatMap(unsubscribed => results.getAndSet(Vector.empty).map(r => if (unsubscribed) None else Some(r)))

  /**
    * Takes the confirmed records back with a poll, and holds them again. Records whose renewal was
    * not confirmed are lost, and records the poll returned which are not held are released. If the
    * poll has closed by the time the records are back, their acknowledgements are sent
    * immediately, as `consume` may have ended.
    */
  def takeBack(confirmed: Set[TopicPartitionOffset]): F[Unit] =
    pollRenewed(confirmed).flatMap { case (returned, lock) =>
      for {
        at <- F.monotonic
        unknown <- state.modify(_.endRenewal(returned, lock, at))
        _ <- release(unknown).whenA(unknown.nonEmpty)
        open <- state.get.map(_.open)
        _ <- send.unlessA(open)
      } yield ()
    }

  /**
    * Polls the renewed records back, after giving the Java consumer the acknowledgements of the
    * records it holds, which its poll requires. The poll fetches nothing, as no renewal is pending
    * anymore. Nothing is polled if no renewal was confirmed, or if the consumer has unsubscribed
    * since the check, as Java's poll would throw, and the records were released along with the
    * subscription.
    */
  private def pollRenewed(
    confirmed: Set[TopicPartitionOffset]
  ): F[(Vector[KafkaByteConsumerRecord], Option[FiniteDuration])] =
    step(_.takeUnsent) { (consumer, acks) =>
      recordIn(consumer, acks)
      if (confirmed.isEmpty || consumer.subscription().isEmpty) (Vector.empty, None)
      else pollRecords(consumer, Duration.Zero)
    }

  /**
    * Releases records which are not held, and sends the releases with an asynchronous commit.
    */
  private def release(records: Vector[KafkaByteConsumerRecord]): F[Unit] =
    step(s => (s, records)) { (consumer, records) =>
      recordIn(consumer, records.map(_ -> AcknowledgeType.Release))
      consumer.commitAsync()
    }

  /**
    * Takes the acknowledgements to send with `take`, and sends them with an asynchronous commit.
    * Only commits if there is something to send, as in implicit mode a commit would accept the
    * poll.
    */
  private def sending(take: ShareConsumerState => (ShareConsumerState, Acks)): F[Unit] =
    step(take) { (consumer, acks) =>
      if (acks.nonEmpty) {
        recordIn(consumer, acks)
        consumer.commitAsync()
      }
    }

  /**
    * Takes something from the state with `take`, and gives it to the Java consumer with `give`.
    * Runs holding the mutex and uncancelable, so that nothing is lost in between.
    */
  private def step[A, B](take: ShareConsumerState => (ShareConsumerState, A))(
    give: (KafkaByteShareConsumer, A) => B
  ): F[B] =
    mutex.lock.surround(state.modify(take).flatMap(a => withConsumer.blocking(give(_, a)))).uncancelable

}

private[kafka] object ShareConsumerCore {

  def of[F[_]](
    withConsumer: WithShareConsumer[F],
    results: Ref[F, Results]
  )(implicit F: Async[F]): F[ShareConsumerCore[F]] =
    (Mutex[F], SignallingRef.of[F, ShareConsumerState](ShareConsumerState.empty))
      .mapN(new ShareConsumerCore(withConsumer, _, _, results))

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
  private def recordIn(consumer: KafkaByteShareConsumer, acks: Acks): Unit =
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

}
