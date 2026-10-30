/*
 * Copyright 2018 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka.internal.actor

import java.util.concurrent.ConcurrentLinkedQueue

import scala.concurrent.duration.*

import cats.effect.Async
import cats.syntax.all.*
import fs2.kafka.*
import fs2.kafka.internal.converters.collection.*
import fs2.kafka.internal.syntax.*
import fs2.kafka.internal.WithShareConsumer

import org.apache.kafka.clients.consumer.AcknowledgementCommitCallback
import org.apache.kafka.common.{KafkaException, TopicPartition}
import org.apache.kafka.common.errors.TimeoutException

/**
  * Stages acknowledgements with Java's `acknowledge` and sends them, for
  * [[KafkaShareConsumerActor]], which decides what to commit. Java reports the results of sent
  * acknowledgements to one callback, during later consumer calls; they are collected for the actor
  * to [[drain]].
  */
final private[actor] class ShareCommitClient[F[_]](
  withConsumer: WithShareConsumer[F]
)(implicit F: Async[F]) {

  import ShareCommits.*
  import ShareTimeouts.acquisitionLock

  private val completions =
    new ConcurrentLinkedQueue[(TopicPartition, Set[Long], Option[KafkaException])]()

  private val callback: AcknowledgementCommitCallback = (offsets, error) => {
    val reported = Option(error).map {
      case kafka: KafkaException => kafka
      case other                 => new KafkaException(other)
    }
    offsets
      .asScala
      .foreach { case (partition, values) =>
        completions.add((partition.topicPartition, values.asScala.map(_.longValue).toSet, reported))
      }
  }

  // Registers the callback which collects the results of sent acknowledgements.
  def install: F[Unit] = withConsumer.blocking(_.setAcknowledgementCommitCallback(callback))

  // The results Java reported since the last drain, by partition and offsets.
  def drain: F[Vector[(TopicPartition, Set[Long], Option[KafkaException])]] =
    F.delay(Iterator.continually(completions.poll()).takeWhile(_ != null).toVector)

  def commitSync(staged: Vector[Staged[F]], timeout: FiniteDuration): F[CommitResult] =
    withConsumer
      .blocking { consumer =>
        // Submit whatever was staged, even if staging fails part-way.
        val staging = scala.util.Try(stage(consumer, staged))
        val results = commitResult(consumer.commitSync(timeout.toJava))
        staging.get
        results.map { case (partition, error) =>
          partition -> error.map {
            case _: TimeoutException =>
              ShareCommitTimeoutException(timeout, offsetsOf(staged, Some(partition)))
            case other => other
          }
        }
      }
      .adaptError { case _: TimeoutException =>
        ShareCommitTimeoutException(timeout, offsetsOf(staged, None))
      }

  /**
    * Sends the acknowledgements with Java's `commitAsync`, without waiting for their results.
    */
  def commitAsync(staged: Vector[Staged[F]]): F[Unit] =
    withConsumer.blocking { consumer =>
      // Submit whatever was staged, even if staging fails part-way.
      try stage(consumer, staged)
      finally consumer.commitAsync()
    }

  /**
    * Stages the acknowledgements and polls, so that Java sends them with the poll's fetch, as it
    * does between two polls of its own. Returns the polled records with the lock, or `None` if the
    * consumer is unsubscribed. Fails if staging fails, after submitting whatever was staged.
    */
  def pollWithAcknowledgements(
    staged: Vector[Staged[F]],
    pollTimeout: FiniteDuration
  ): F[Option[(Vector[KafkaByteConsumerRecord], Option[FiniteDuration])]] =
    withConsumer.blocking { consumer =>
      scala.util.Try(stage(consumer, staged)) match {
        // Java cannot poll with records left undecided.
        case scala.util.Failure(error) =>
          consumer.commitAsync()
          throw error
        case scala.util.Success(_) if consumer.subscription().isEmpty =>
          consumer.commitAsync()
          None
        case scala.util.Success(_) =>
          val records = consumer.poll(pollTimeout.toJava).iterator().asScala.toVector
          Some(records -> acquisitionLock(consumer))
      }
    }

  // Lets Java report the results it has received, as it does only during consumer calls.
  def nudge: F[Unit] = withConsumer.blocking(_.commitAsync())

  private def stage(consumer: KafkaByteShareConsumer, staged: Vector[Staged[F]]): Unit =
    staged.foreach { case (ack, record) =>
      consumer.acknowledge(record.record, ack.acknowledgeType.asJava)
    }

}
