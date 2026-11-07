/*
 * Copyright 2018 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka

import cats.Reducible
import cats.data.NonEmptyVector
import cats.effect.Async
import cats.effect.Resource
import cats.effect.instances.spawn.*
import cats.syntax.all.*
import fs2.Chunk
import fs2.kafka.consumer.KafkaShareSubscription
import fs2.kafka.consumer.MkShareConsumer
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.config.ConfigException
import scala.collection.immutable.SortedSet

/**
  * Share consumers of the same share group, each with its own Java consumer and its own loop, so
  * that polls are processed in parallel across consumers, one at a time within each.<br><br>
  *
  * Experimental: this API targets Kafka's share groups.
  */
sealed abstract class KafkaShareConsumers[F[_], K, V] extends KafkaShareSubscription[F] {

  /**
    * The share consumers, in the order they were created.
    */
  def consumers: NonEmptyVector[KafkaShareConsumer[F, K, V]]

  /**
    * Runs [[KafkaShareConsumer.consume]] on every consumer until [[stopConsuming]]. If one fails,
    * the others are canceled, which releases the records they hold, and the error is raised.
    */
  def consume(process: Chunk[AcknowledgeableConsumerRecord[F, K, V]] => F[Unit]): F[Unit]

  /**
    * Runs [[KafkaShareConsumer.autoConsume]] on every consumer until [[stopConsuming]].
    */
  def autoConsume(process: Chunk[ConsumerRecord[K, V]] => F[Unit]): F[Unit]

  def stopConsuming: F[Unit]

}

object KafkaShareConsumers {

  /**
    * Creates `consumers` share consumers with the same settings; a client id gets the consumer's
    * index as a suffix.
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
        .parTraverse(index => KafkaShareConsumer.resource(indexed(settings, index)))
        .map(new Impl(_))

  private def indexed[F[_], K, V](
    settings: ShareConsumerSettings[F, K, V],
    index: Int
  ): ShareConsumerSettings[F, K, V] =
    settings.properties
      .get(ConsumerConfig.CLIENT_ID_CONFIG)
      .fold(settings)(id => settings.withClientId(s"$id-$index"))

  final private class Impl[F[_], K, V](
    override val consumers: NonEmptyVector[KafkaShareConsumer[F, K, V]]
  )(implicit F: Async[F])
    extends KafkaShareConsumers[F, K, V] {

    override def subscribe[G[_]: Reducible](topics: G[String]): F[Unit] =
      consumers.traverse_(_.subscribe(topics))

    override def unsubscribe: F[Unit] = consumers.traverse_(_.unsubscribe)

    override def subscription: F[SortedSet[String]] =
      consumers.traverse(_.subscription).map(_.reduceLeft(_ ++ _))

    override def consume(
      process: Chunk[AcknowledgeableConsumerRecord[F, K, V]] => F[Unit]
    ): F[Unit] = consumers.parTraverse_(_.consume(process))

    override def autoConsume(process: Chunk[ConsumerRecord[K, V]] => F[Unit]): F[Unit] =
      consumers.parTraverse_(_.autoConsume(process))

    override def stopConsuming: F[Unit] = consumers.traverse_(_.stopConsuming)

  }

}
