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
  import ShareTimeouts.acquisitionLock

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
      val confirmations = new Confirmations
      // Submit whatever was staged, even if staging fails part-way.
      val submit = withConsumer.blocking { consumer =>
        consumer.setAcknowledgementCommitCallback(confirmations.callback)
        try stage(consumer, staged)
        finally consumer.commitAsync()
      }
      val recent   = recentConfirmations
      val presleep =
        if (recent.isEmpty) Duration.Zero else (recent.min * 3 / 4).min(pollInterval)
      val confirm =
        submit *> F.sleep(presleep).whenA(presleep > Duration.Zero) *>
          confirmations.await(staged)
      confirm.timeoutTo(
        timeout,
        F.raiseError[CommitResult](ShareCommitTimeoutException(timeout, offsetsOf(staged, None)))
      )
    }

  /**
    * Stages the acknowledgements and polls, so that Java sends them with the poll's fetch, as it
    * does between two polls of its own. Java usually confirms them during that poll; any it has not
    * confirmed yet are awaited as in [[commitAsync]]. Returns the result of the acknowledgements,
    * and the polled records with the lock, unless staging failed or the consumer is unsubscribed.
    */
  def pollWithAcknowledgements(
    staged: Vector[Staged[F]],
    timeout: FiniteDuration,
    pollTimeout: FiniteDuration
  ): F[
    (
      Either[Throwable, CommitResult],
      Option[(Vector[KafkaByteConsumerRecord], Option[FiniteDuration])]
    )
  ] =
    F.defer {
      val confirmations = new Confirmations
      withConsumer
        .blocking { consumer =>
          consumer.setAcknowledgementCommitCallback(confirmations.callback)
          scala.util.Try(stage(consumer, staged)) match {
            // Submit whatever was staged, as Java cannot poll with records left undecided.
            case scala.util.Failure(error) =>
              consumer.commitAsync()
              Left(error)
            case scala.util.Success(_) if consumer.subscription().isEmpty =>
              consumer.commitAsync()
              Right(None)
            case scala.util.Success(_) =>
              val records = consumer.poll(pollTimeout.toJava).iterator().asScala.toVector
              Right(Some(records -> acquisitionLock(consumer)))
          }
        }
        .flatMap {
          case Left(error)   => F.pure(Left(error) -> None)
          case Right(polled) =>
            confirmations
              .await(staged)
              .timeoutTo(
                timeout,
                F.raiseError[CommitResult](
                  ShareCommitTimeoutException(timeout, offsetsOf(staged, None))
                )
              )
              .attempt
              .map(_ -> polled)
        }
    }

  /**
    * Collects the results Java reports to the acknowledgement commit callback, and waits for those
    * of the staged acknowledgements.
    */
  private class Confirmations {

    private val completions =
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

    private val maxBackoff = pollInterval

    private def drain(
      remaining: Set[ShareState.Id],
      results: CommitResult
    ): F[(Set[ShareState.Id], CommitResult)] =
      F.delay {
        Iterator
          .continually(completions.poll())
          .takeWhile(_ != null)
          .foldLeft((remaining, results)) { case ((pending, merged), (partition, offsets, error)) =>
            val ids = offsets.map(partition -> _)
            if (ids.exists(pending.contains))
              (pending -- ids, mergeResults(merged, Map(partition -> error)))
            else (pending, merged)
          }
      }

    private def loop(
      remaining: Set[ShareState.Id],
      results: CommitResult,
      backoff: FiniteDuration
    ): F[CommitResult] =
      drain(remaining, results).flatMap { case (pending, merged) =>
        if (pending.isEmpty) F.pure(merged)
        else
          F.sleep(backoff) *> withConsumer.blocking(_.commitAsync()) *>
            loop(pending, merged, (backoff * 2).min(maxBackoff))
      }

    def await(staged: Vector[Staged[F]]): F[CommitResult] =
      loop(staged.map(_._1.id).toSet, Map.empty, 100.micros.min(maxBackoff))

  }

  private def stage(consumer: KafkaByteShareConsumer, staged: Vector[Staged[F]]): Unit =
    staged.foreach { case (ack, record) =>
      consumer.acknowledge(record.record, ack.acknowledgeType.asJava)
    }

}
