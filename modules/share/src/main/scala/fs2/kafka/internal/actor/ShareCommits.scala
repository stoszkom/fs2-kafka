/*
 * Copyright 2018 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka.internal.actor

import cats.effect.{Async, Ref}
import cats.syntax.all.*
import fs2.kafka.*
import fs2.kafka.internal.converters.collection.*
import fs2.Chunk

import org.apache.kafka.common.{KafkaException, TopicIdPartition, TopicPartition}

private[kafka] object ShareCommits {

  type CommitResult = Map[TopicPartition, Option[KafkaException]]

  type Callback[F[_]] = Either[Throwable, CommitResult] => F[Unit]

  // An acknowledgement with the in-flight record it applies to.
  type Staged[F[_]] = (ShareRequest.Ack, InFlightRecord[F])

  // The acknowledgements of one commit request, by what happens to them in a round.
  final case class Split[F[_]](
    staged: Vector[Staged[F]] = Vector.empty[Staged[F]],
    deferred: Vector[ShareRequest.Ack] = Vector.empty,
    invalid: Vector[ShareRequest.Ack] = Vector.empty,
    later: Vector[ShareRequest.Ack] = Vector.empty
  ) {

    def count: Int =
      List(staged.nonEmpty, deferred.nonEmpty, invalid.nonEmpty, later.nonEmpty).count(identity)

  }

  // The commit requests of one round, with the records already claimed by a decision.
  final case class Round[F[_]](
    claimed: Set[ShareState.Id] = Set.empty,
    staged: Vector[(Vector[Staged[F]], Callback[F])] =
      Vector.empty[(Vector[Staged[F]], Callback[F])],
    deferred: Vector[ShareRequest.Commit[F]] = Vector.empty[ShareRequest.Commit[F]],
    later: Vector[ShareRequest.Commit[F]] = Vector.empty[ShareRequest.Commit[F]]
  )

  def when[A](condition: Boolean)(value: => A): Option[A] =
    if (condition) Some(value) else None

  def firstError(results: CommitResult): Option[KafkaException] =
    results.values.flatten.headOption

  def notInFlight(acks: Vector[ShareRequest.Ack]): CommitResult =
    acks
      .map(ack =>
        ack.topicPartition ->
          Option[KafkaException](RecordNotInFlightException(ack.topicPartition, ack.offset))
      )
      .toMap

  def offsetsOf[F[_]](
    staged: Vector[Staged[F]],
    partition: Option[TopicPartition]
  ): Map[TopicPartition, Map[Long, AcknowledgeType]] =
    staged
      .map(_._1)
      .filter(ack => partition.forall(_ == ack.topicPartition))
      .groupBy(_.topicPartition)
      .map { case (p, acks) => p -> acks.map(ack => ack.offset -> ack.acknowledgeType).toMap }

  def fromAcknowledged[F[_]](
    offsets: Chunk[AcknowledgedOffset[F]]
  ): Chunk[ShareRequest.Ack] =
    offsets.map(a =>
      ShareRequest.Ack(a.topicPartition, a.offset, Some(a.deliveryCount), a.acknowledgeType)
    )

  private[kafka] def fromOffsets(
    offsets: Map[TopicPartition, Map[Long, AcknowledgeType]]
  ): Chunk[ShareRequest.Ack] =
    Chunk.from(
      for {
        (partition, acknowledgements) <- offsets.toVector
        (offset, acknowledgeType)     <- acknowledgements.toVector
      } yield ShareRequest.Ack(partition, offset, None, acknowledgeType)
    )

  def commitResult(
    results: java.util.Map[TopicIdPartition, java.util.Optional[KafkaException]]
  ): CommitResult =
    results
      .asScala
      .iterator
      .map { case (partition, error) =>
        partition.topicPartition -> (if (error.isPresent) Some(error.get) else None)
      }
      .toMap

  def joinCallbacks[F[_]](
    callback: Callback[F],
    parts: Int
  )(implicit F: Async[F]): F[Vector[Callback[F]]] =
    if (parts <= 1) F.pure(Vector.fill(parts)(callback))
    else
      Ref
        .of[F, (Int, Either[Throwable, CommitResult])]((parts, Right(Map.empty)))
        .map { state =>
          Vector.fill(parts) { (result: Either[Throwable, CommitResult]) =>
            state
              .modify { case (remaining, joined) =>
                val next = (joined, result) match {
                  case (Left(failure), _)           => Left(failure)
                  case (Right(_), Left(failure))    => Left(failure)
                  case (Right(all), Right(results)) => Right(mergeResults(all, results))
                }
                ((remaining - 1, next), if (remaining == 1) callback(next) else F.unit)
              }
              .flatten
          }
        }

  private[kafka] def mergeResults(left: CommitResult, right: CommitResult): CommitResult =
    right.foldLeft(left) { case (merged, (partition, error)) =>
      merged.updated(partition, merged.get(partition).flatten.orElse(error))
    }

}
