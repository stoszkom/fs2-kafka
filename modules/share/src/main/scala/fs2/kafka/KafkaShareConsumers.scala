/*
 * Copyright 2018 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka

import scala.collection.immutable.SortedSet

import cats.data.NonEmptyVector
import cats.effect.instances.spawn.*
import cats.effect.Async
import cats.syntax.all.*
import cats.Reducible
import fs2.kafka.consumer.{KafkaConsumerLifecycle, KafkaShareSubscription}
import fs2.Stream

/**
  * Share consumers of the same share group. Each has its own Java consumer underneath.
  *
  * Experimental: this API targets Kafka's share groups and may change in minor releases.
  */
sealed abstract class KafkaShareConsumers[F[_], K, V]
    extends KafkaShareSubscription[F]
    with KafkaConsumerLifecycle[F] {

  /**
    * The share consumers, in the order they were created.
    */
  def consumers: NonEmptyVector[KafkaShareConsumer[F, K, V]]

  /**
    * `Stream` of the records of all consumers, merged into a single `Stream` of
    * [[AcknowledgeableConsumerRecord]]s. A record counts as handed out only once it leaves the
    * merge, so records still buffered in the merge when this `Stream` ends are released.<br><br>
    *
    * If you want to process the records of each consumer in its own pipeline, use
    * [[perConsumerRecords]] instead.
    *
    * @note
    *   you have to first use `subscribe` before using this `Stream`. If you forgot to subscribe,
    *   there will be a [[NotSubscribedException]] raised in the `Stream`. The record `Stream` of
    *   each consumer can be started only once, so use either this `Stream` or
    *   [[perConsumerRecords]], once, or there will be a [[StreamAlreadyStartedException]] raised.
    * @see
    *   [[perConsumerRecords]]
    */
  def records: Stream[F, AcknowledgeableConsumerRecord[F, K, V]]

  /**
    * `Stream` where the elements themselves are `Stream`s of the records of a single consumer.
    * These `Stream`s will have to be processed in parallel, using `parJoin` or `parJoinUnbounded`,
    * for example `perConsumerRecords.map(_.parEvalMapUnordered(8)(process)).parJoinUnbounded`. Note
    * that when using `parJoin(n)` and `n` is smaller than the number of consumers, the remaining
    * consumers won't consume records until one of the processed `Stream`s ends. For that reason,
    * prefer `parJoinUnbounded`.<br><br>
    *
    * If you want the records of all consumers in a single `Stream`, use [[records]] instead.
    *
    * @note
    *   you have to first use `subscribe` before using this `Stream`. If you forgot to subscribe,
    *   there will be a [[NotSubscribedException]] raised in the `Stream`. The record `Stream` of
    *   each consumer can be started only once, so use either this `Stream` or [[records]], once, or
    *   there will be a [[StreamAlreadyStartedException]] raised.
    * @see
    *   [[records]]
    */
  def perConsumerRecords: Stream[F, Stream[F, AcknowledgeableConsumerRecord[F, K, V]]]

  /**
    * Stops polling new records on every consumer, see [[KafkaShareConsumer.stopConsuming]].
    */
  def stopConsuming: F[Unit]

}

object KafkaShareConsumers {

  private[kafka] def apply[F[_], K, V](
    values: NonEmptyVector[KafkaShareConsumer[F, K, V]]
  )(implicit F: Async[F]): KafkaShareConsumers[F, K, V] =
    new KafkaShareConsumers[F, K, V] {

      override val consumers: NonEmptyVector[KafkaShareConsumer[F, K, V]] = values

      override def subscribe[G[_]: Reducible](topics: G[String]): F[Unit] =
        values.traverse_(_.subscribe(topics))

      override def unsubscribe: F[Unit] = values.traverse_(_.unsubscribe)

      override def subscription: F[SortedSet[String]] = values.head.subscription

      override def records: Stream[F, AcknowledgeableConsumerRecord[F, K, V]] =
        Stream
          .emits(values.toVector)
          .map(_.markableRecords)
          .parJoin(values.length)
          .through(values.head.handOut)

      override def perConsumerRecords
        : Stream[F, Stream[F, AcknowledgeableConsumerRecord[F, K, V]]] =
        Stream.emits(values.toVector.map(_.records))

      override def stopConsuming: F[Unit] = values.parTraverse_(_.stopConsuming)

      override def terminate: F[Unit] = values.parTraverse_(_.terminate)

      override def awaitTermination: F[Unit] = values.parTraverse_(_.awaitTermination)

      override def toString: String = s"KafkaShareConsumers(${values.length})"

    }

}
