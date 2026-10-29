/*
 * Copyright 2018 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka.internal.actor

import java.util.concurrent.ConcurrentLinkedQueue

import scala.concurrent.duration.*

import cats.effect.syntax.all.*
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
  * Stages acknowledgements with Java's `acknowledge` and commits them, for
  * [[KafkaShareConsumerActor]], which decides what to commit.
  */
final private[actor] class ShareCommitClient[F[_]](
  withConsumer: WithShareConsumer[F],
  pollInterval: FiniteDuration
)(implicit F: Async[F]) {

  import ShareCommits.*

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
    * Commits with Java's `commitAsync` and waits for the callback. Java runs it only during
    * consumer calls, so the wait calls `commitAsync` again, in growing steps.
    */
  def commitAsync(
    staged: Vector[Staged[F]],
    timeout: FiniteDuration,
    recentConfirmations: Vector[FiniteDuration]
  ): F[CommitResult] =
    F.defer {
      val completions =
        new ConcurrentLinkedQueue[(TopicPartition, Set[Long], Option[KafkaException])]()
      val callback: AcknowledgementCommitCallback = (offsets, error) => {
        val reported = Option(error).map {
          case kafka: KafkaException => kafka
          case other                 => new KafkaException(other)
        }
        offsets
          .asScala
          .foreach { case (partition, values) =>
            completions.add(
              (partition.topicPartition, values.asScala.map(_.longValue).toSet, reported)
            )
          }
      }
      val maxBackoff = pollInterval

      def drain(
        remaining: Set[ShareState.Id],
        results: CommitResult
      ): F[(Set[ShareState.Id], CommitResult)] =
        F.delay {
          Iterator
            .continually(completions.poll())
            .takeWhile(_ != null)
            .foldLeft((remaining, results)) {
              case ((pending, merged), (partition, offsets, error)) =>
                val ids = offsets.map(partition -> _)
                if (ids.exists(pending.contains))
                  (pending -- ids, mergeResults(merged, Map(partition -> error)))
                else (pending, merged)
            }
        }

      def await(
        remaining: Set[ShareState.Id],
        results: CommitResult,
        backoff: FiniteDuration
      ): F[CommitResult] =
        drain(remaining, results).flatMap { case (pending, merged) =>
          if (pending.isEmpty) F.pure(merged)
          else
            F.sleep(backoff) *> withConsumer.blocking(_.commitAsync()) *>
              await(pending, merged, (backoff * 2).min(maxBackoff))
        }

      // Submit whatever was staged, even if staging fails part-way.
      val submit = withConsumer.blocking { consumer =>
        consumer.setAcknowledgementCommitCallback(callback)
        try stage(consumer, staged)
        finally consumer.commitAsync()
      }
      val recent   = recentConfirmations
      val presleep =
        if (recent.isEmpty) Duration.Zero else (recent.min * 3 / 4).min(pollInterval)
      val confirm =
        submit *> F.sleep(presleep).whenA(presleep > Duration.Zero) *>
          await(staged.map(_._1.id).toSet, Map.empty, 100.micros.min(maxBackoff))
      confirm.timeoutTo(
        timeout,
        F.raiseError[CommitResult](ShareCommitTimeoutException(timeout, offsetsOf(staged, None)))
      )
    }

  private def stage(consumer: KafkaByteShareConsumer, staged: Vector[Staged[F]]): Unit =
    staged.foreach { case (ack, record) =>
      consumer.acknowledge(record.record, ack.acknowledgeType.asJava)
    }

}
