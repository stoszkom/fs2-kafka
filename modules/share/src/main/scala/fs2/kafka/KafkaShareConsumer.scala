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
import fs2.kafka.consumer.*
import fs2.kafka.internal.actor.{KafkaShareConsumerActor, ShareTimeouts}
import fs2.kafka.internal.converters.collection.*
import fs2.kafka.internal.syntax.*
import fs2.kafka.internal.WithShareConsumer
import fs2.Stream

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.{KafkaException, Metric, MetricName, TopicPartition, Uuid}
import org.apache.kafka.common.config.ConfigException

/**
  * [[KafkaShareConsumer]] is a consumer in a Kafka share group, wrapping Java's
  * `KafkaShareConsumer`. It provides a single top-level [[stream]] of records, in the order they
  * were polled.<br><br>
  *
  * Records come as [[AcknowledgeableConsumerRecord]]s. Acknowledging one stages the decision in the
  * Java consumer, as Java's `acknowledge` does, one record at a time or a chunk at once with
  * [[acknowledgeChunks]]. Staged decisions are sent by the next poll, after the
  * [[ShareConsumerSettings.maxAcknowledgementDelay]], or by [[commitSync]], which reports the
  * result, or [[commitAsync]].<br><br>
  *
  * In explicit acknowledgement mode, the default, the consumer polls again once every record of the
  * previous poll has a decision. A record not acknowledged within its acquisition lock is delivered
  * again, unless it is renewed, see [[ShareConsumerSettings.withAutoRenewal]].<br><br>
  *
  * In implicit acknowledgement mode, the next poll or commit accepts the previous poll, and
  * acknowledging fails with an [[ImplicitAcknowledgementException]]. The consumer polls again once
  * the stream took a poll, so this is at-most-once.<br><br>
  *
  * A second stream fails with a [[StreamAlreadyStartedException]]. To consume again, create a new
  * consumer. For several consumers in one share group, see [[KafkaShareConsumers]].<br><br>
  *
  * Experimental: this API targets Kafka's share groups and may change in minor releases.
  */
sealed abstract class KafkaShareConsumer[F[_], K, V]
    extends KafkaShareConsume[F, K, V]
    with KafkaShareSubscription[F]
    with KafkaMetrics[F]
    with KafkaConsumerLifecycle[F] {

  /**
    * Returns the settings used to create the consumer instance.
    */
  def settings: ShareConsumerSettings[F, K, V]

  /**
    * Sends every staged decision and waits for the result per partition, as Java's `commitSync`
    * does, within [[ShareConsumerSettings.commitTimeout]]. Decisions waiting for a renewed record
    * are not included. In implicit mode, it accepts the latest poll once the stream took it, which
    * the next poll usually did already, giving an empty result. A partition the broker did not
    * confirm in time has Kafka's `TimeoutException` as its result; a commit that times out before
    * it is sent fails with a [[ShareCommitTimeoutException]].
    */
  def commitSync: F[Map[TopicPartition, Option[KafkaException]]]

  /**
    * [[commitSync]] with the given timeout, which must be positive. With automatic renewal, a
    * timeout longer than [[ShareConsumerSettings.commitTimeout]] can delay renewals past the lock.
    */
  def commitSync(timeout: FiniteDuration): F[Map[TopicPartition, Option[KafkaException]]]

  /**
    * Sends every staged decision without waiting for the broker, as Java's `commitAsync` does.
    * Results are not reported: a decision the broker rejects shows as the record being delivered
    * again.
    */
  def commitAsync: F[Unit]

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
    * Like [[stream]], but one stream per poll, which hands the poll out, so that merged polls of
    * several consumers are handed out after the merge.
    */
  private[kafka] def polls: Stream[F, Stream[F, AcknowledgeableConsumerRecord[F, K, V]]]

}

object KafkaShareConsumer {

  private def createKafkaShareConsumer[F[_], K, V](
    _settings: ShareConsumerSettings[F, K, V],
    actor: KafkaShareConsumerActor[F, K, V],
    withConsumer: WithShareConsumer[F],
    id: Int
  ): KafkaShareConsumer[F, K, V] =
    new KafkaShareConsumer[F, K, V] {

      override val settings: ShareConsumerSettings[F, K, V] = _settings

      override def commitSync: F[Map[TopicPartition, Option[KafkaException]]] =
        actor.commitSync(settings.commitTimeout)

      override def commitSync(
        timeout: FiniteDuration
      ): F[Map[TopicPartition, Option[KafkaException]]] =
        actor.commitSync(timeout)

      override def commitAsync: F[Unit] = actor.commitAsync

      override private[kafka] def polls
        : Stream[F, Stream[F, AcknowledgeableConsumerRecord[F, K, V]]] =
        actor.polls

      override def stream: Stream[F, AcknowledgeableConsumerRecord[F, K, V]] = actor.records

      override def stopConsuming: F[Unit] = actor.stopConsuming

      override def subscribe[G[_]: Reducible](topics: G[String]): F[Unit] =
        withConsumer.blocking(_.subscribe(topics.toList.asJava))

      override def unsubscribe: F[Unit] = withConsumer.blocking(_.unsubscribe())

      override def subscription: F[SortedSet[String]] =
        withConsumer.blocking(_.subscription().toSortedSet)

      override def metrics: F[Map[MetricName, Metric]] =
        withConsumer.blocking(_.metrics().asScala.toMap)

      override def acquisitionLockTimeout: F[Option[FiniteDuration]] =
        withConsumer.blocking(ShareTimeouts.acquisitionLock)

      override def clientInstanceId(timeout: FiniteDuration): F[Uuid] =
        withConsumer.blocking(_.clientInstanceId(timeout.toJava))

      override def terminate: F[Unit] = actor.terminate

      override def awaitTermination: F[Unit] = actor.awaitTermination

      override def toString: String = "KafkaShareConsumer$" + id

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
      id                <- Resource.eval(F.delay(new Object().hashCode))
      withConsumer      <- WithShareConsumer(mk, settings)
      actor             <- KafkaShareConsumerActor.resource(
                 settings,
                 keyDeserializer,
                 valueDeserializer,
                 withConsumer,
                 id
               )
    } yield createKafkaShareConsumer(settings, actor, withConsumer, id)

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
