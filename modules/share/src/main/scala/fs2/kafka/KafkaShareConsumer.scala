/*
 * Copyright 2018 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka

import scala.collection.immutable.SortedSet
import scala.concurrent.duration.*

import cats.data.NonEmptyVector
import cats.effect.{Async, Resource}
import cats.effect.instances.spawn.*
import cats.syntax.all.*
import cats.Reducible
import fs2.{Pipe, Stream}
import fs2.kafka.consumer.*
import fs2.kafka.internal.actor.{KafkaShareConsumerActor, ShareCommits, ShareTimeouts}
import fs2.kafka.internal.converters.collection.*
import fs2.kafka.internal.syntax.*
import fs2.kafka.internal.WithShareConsumer

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.{Metric, MetricName, TopicPartition, Uuid}
import org.apache.kafka.common.config.ConfigException

/**
  * [[KafkaShareConsumer]] is a consumer in a Kafka share group, wrapping Java's
  * `KafkaShareConsumer`. It provides a single top-level [[stream]] of records, in the order they
  * were polled.<br><br>
  *
  * Records come as [[AcknowledgeableConsumerRecord]]s. Their [[AcknowledgeableOffset]]s are
  * acknowledged and committed one at a time, in batches with [[AcknowledgedOffsetBatch]], or with
  * pipes like [[commitPollWithin]].<br><br>
  *
  * In explicit acknowledgement mode, the default, every record of a poll needs an acknowledgement
  * before the next poll. A record not acknowledged within its acquisition lock is delivered again,
  * unless it is renewed, see [[ShareConsumerSettings.withAutoRenewal]].<br><br>
  *
  * In implicit acknowledgement mode, the next poll accepts the records of the previous one, even
  * while they are still processed (at-most-once), and acknowledging fails with an
  * [[ImplicitAcknowledgementException]].<br><br>
  *
  * A second stream fails with a [[StreamAlreadyStartedException]]. To consume again, create a new
  * consumer. For several consumers in one share group, see [[KafkaShareConsumers]].<br><br>
  *
  * Experimental: this API targets Kafka's share groups and may change in minor releases.
  */
sealed abstract class KafkaShareConsumer[F[_], K, V]
    extends KafkaShareConsume[F, K, V]
    with KafkaShareSubscription[F]
    with KafkaShareCommit[F]
    with KafkaMetrics[F]
    with KafkaConsumerLifecycle[F] {

  /**
    * Returns the settings used to create the consumer instance.
    */
  def settings: ShareConsumerSettings[F, K, V]

  /**
    * Returns the committer used by the [[AcknowledgeableOffset]]s of this consumer.
    */
  def committer: KafkaShareCommitter[F]

  /**
    * Returns the broker's acquisition lock duration, or `None` until it is known.
    *
    * @see
    *   org.apache.kafka.clients.consumer.KafkaShareConsumer#acquisitionLockTimeoutMs
    */
  def acquisitionLockTimeout: F[Option[FiniteDuration]]

  /**
    * Returns the client instance id used for client metrics.
    *
    * @see
    *   org.apache.kafka.clients.consumer.KafkaShareConsumer#clientInstanceId
    */
  def clientInstanceId(timeout: FiniteDuration): F[Uuid]

  /**
    * Like [[stream]], but leaves marking each record as handed out to the caller.
    */
  private[kafka] def markableRecords: Stream[F, (AcknowledgeableConsumerRecord[F, K, V], F[Unit])]

  /**
    * Marks records as handed out as they leave the `Stream`. In explicit acknowledgement mode, a
    * whole chunk is marked at once, which keeps a poll in one chunk; records a `take` skips are
    * then released when the consumer closes. In implicit mode, where handing out a poll accepts it,
    * each record is marked as it leaves.
    */
  private[kafka] def handOut: Pipe[
    F,
    (AcknowledgeableConsumerRecord[F, K, V], F[Unit]),
    AcknowledgeableConsumerRecord[F, K, V]
  ]

}

object KafkaShareConsumer {

  private def createKafkaShareConsumer[F[_], K, V](
    _settings: ShareConsumerSettings[F, K, V],
    actor: KafkaShareConsumerActor[F, K, V],
    withConsumer: WithShareConsumer[F]
  )(implicit F: Async[F]): KafkaShareConsumer[F, K, V] =
    new KafkaShareConsumer[F, K, V] {

      override val settings: ShareConsumerSettings[F, K, V] = _settings

      override val committer: KafkaShareCommitter[F] = actor.committer

      override private[kafka] def markableRecords
        : Stream[F, (AcknowledgeableConsumerRecord[F, K, V], F[Unit])] =
        actor.entries

      override private[kafka] def handOut: Pipe[
        F,
        (AcknowledgeableConsumerRecord[F, K, V], F[Unit]),
        AcknowledgeableConsumerRecord[F, K, V]
      ] =
        if (actor.implicitAcknowledgement)
          _.evalMap { case (record, markEmitted) => markEmitted.as(record) }
        else _.evalMapChunk { case (record, markEmitted) => markEmitted.as(record) }

      override def stream: Stream[F, AcknowledgeableConsumerRecord[F, K, V]] =
        markableRecords.through(handOut)

      override def stopConsuming: F[Unit] = actor.stopConsuming

      override def subscribe[G[_]: Reducible](topics: G[String]): F[Unit] =
        withConsumer.blocking(_.subscribe(topics.toList.asJava))

      override def unsubscribe: F[Unit] = withConsumer.blocking(_.unsubscribe())

      override def subscription: F[SortedSet[String]] =
        withConsumer.blocking(_.subscription().toSortedSet)

      override def commitAsync(
        offsets: Map[TopicPartition, Map[Long, AcknowledgeType]]
      ): F[Unit] = actor.commit(ShareCommits.fromOffsets(offsets), sync = false)

      override def commitSync(
        offsets: Map[TopicPartition, Map[Long, AcknowledgeType]]
      ): F[Unit] = actor.commit(ShareCommits.fromOffsets(offsets), sync = true)

      override def metrics: F[Map[MetricName, Metric]] =
        withConsumer.blocking(_.metrics().asScala.toMap)

      override def acquisitionLockTimeout: F[Option[FiniteDuration]] =
        withConsumer.blocking(ShareTimeouts.acquisitionLock)

      override def clientInstanceId(timeout: FiniteDuration): F[Uuid] =
        withConsumer.blocking(_.clientInstanceId(timeout.toJava))

      override def terminate: F[Unit] = actor.terminate

      override def awaitTermination: F[Unit] = actor.awaitTermination

      override def toString: String = "KafkaShareConsumer$" + System.identityHashCode(this)

    }

  /**
    * Creates a new [[KafkaShareConsumer]] in the `Resource` context, using the specified
    * [[ShareConsumerSettings]].
    */
  def resource[F[_], K, V](
    settings: ShareConsumerSettings[F, K, V]
  )(implicit F: Async[F], mk: MkShareConsumer[F]): Resource[F, KafkaShareConsumer[F, K, V]] =
    for {
      keyDeserializer   <- settings.keyDeserializer
      valueDeserializer <- settings.valueDeserializer
      withConsumer      <- WithShareConsumer(mk, settings)
      actor             <-
        KafkaShareConsumerActor.resource(settings, keyDeserializer, valueDeserializer, withConsumer)
    } yield createKafkaShareConsumer(settings, actor, withConsumer)

  /**
    * Creates a new [[KafkaShareConsumer]] in the `Stream` context, using the specified
    * [[ShareConsumerSettings]].
    */
  def stream[F[_], K, V](settings: ShareConsumerSettings[F, K, V])(implicit
    F: Async[F],
    mk: MkShareConsumer[F]
  ): Stream[F, KafkaShareConsumer[F, K, V]] = Stream.resource(resource(settings))

  /**
    * Creates `consumers` [[KafkaShareConsumer]]s in the same share group, in the `Resource`
    * context. When `client.id` is set, consumer `i` uses `client.id-i`. Fails with a
    * `ConfigException` unless `consumers` is positive.
    */
  def resource[F[_], K, V](settings: ShareConsumerSettings[F, K, V], consumers: Int)(implicit
    F: Async[F],
    mk: MkShareConsumer[F]
  ): Resource[F, KafkaShareConsumers[F, K, V]] =
    if (consumers < 1)
      Resource.raiseError[F, KafkaShareConsumers[F, K, V], Throwable](
        new ConfigException(s"consumers must be positive, was $consumers")
      )
    else
      NonEmptyVector
        .fromVectorUnsafe(Vector.range(0, consumers))
        .parTraverse(index => resource(indexed(settings, index)))
        .map(KafkaShareConsumers(_))

  private def indexed[F[_], K, V](
    settings: ShareConsumerSettings[F, K, V],
    index: Int
  ): ShareConsumerSettings[F, K, V] =
    settings
      .properties
      .get(ConsumerConfig.CLIENT_ID_CONFIG)
      .fold(settings)(id => settings.withClientId(s"$id-$index"))

  /**
    * Like `resource(settings, consumers)`, but applies `subscribe` to each consumer and streams
    * their merged [[KafkaShareConsumers.records]].
    */
  def stream[F[_], K, V](settings: ShareConsumerSettings[F, K, V], consumers: Int)(
    subscribe: KafkaShareConsumer[F, K, V] => F[Unit]
  )(implicit
    F: Async[F],
    mk: MkShareConsumer[F]
  ): Stream[F, AcknowledgeableConsumerRecord[F, K, V]] =
    Stream
      .resource(resource(settings, consumers))
      .evalTap(_.consumers.traverse_(subscribe))
      .flatMap(_.records)

  implicit final class StreamOps[F[_], K, V](self: Stream[F, KafkaShareConsumer[F, K, V]]) {

    /**
      * Subscribes the consumer to the specified topics. See
      * [[fs2.kafka.consumer.KafkaShareSubscription#subscribe]].
      */
    def subscribeTo(topic: String, topics: String*): Stream[F, KafkaShareConsumer[F, K, V]] =
      self.evalTap(_.subscribeTo(topic, topics*))

    /**
      * The records of the consumer. See [[fs2.kafka.consumer.KafkaShareConsume#stream]].
      */
    def records: Stream[F, AcknowledgeableConsumerRecord[F, K, V]] = self.flatMap(_.records)

  }

}
