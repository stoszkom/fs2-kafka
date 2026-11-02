/*
 * Copyright 2018 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka.internal.actor

import scala.concurrent.duration.FiniteDuration

import fs2.kafka.{AcknowledgeType, KafkaByteConsumerRecord, RecordNotInFlightException}
import fs2.kafka.internal.syntax.*

/**
  * A delivery of a record held by the consumer.
  *
  * @param record
  *   the Java record to acknowledge it by, from the poll which last returned it
  * @param lockedSince
  *   the estimated start of its lock: its acquisition, or its latest renewal
  * @param heldSince
  *   the estimated start of its processing: when the stream took it, or its latest renewal by the
  *   user; the max processing time counts from it
  */
final private[kafka] case class Held(
  deliveryCount: Short,
  record: KafkaByteConsumerRecord,
  lockedSince: FiniteDuration,
  heldSince: FiniteDuration
)

// A request completed once its decisions for renewing records are staged.
final private[kafka] case class Waiter[F[_]](
  positions: Set[TopicPartitionOffset],
  done: Either[Throwable, Unit] => F[Unit]
)

/**
  * @param held
  *   records whose decision Java has not sent, and renewed records not returned yet
  * @param decided
  *   held records with a staged decision
  * @param renewing
  *   held records whose renewal was sent, which a later poll returns
  * @param deferred
  *   decisions for renewing records, staged once a poll returns them
  * @param lock
  *   the acquisition lock last reported by the broker
  * @param unpulled
  *   the latest poll's records while the stream has not taken it
  * @param pollId
  *   the latest poll offered to the stream, to tell a late hand-out of an earlier one apart
  * @param stagedSince
  *   when the oldest decision staged but not sent was staged
  * @param waiters
  *   requests completed once their decisions in `deferred` are staged
  */
final private[kafka] case class ShareState[F[_]](
  held: Map[TopicPartitionOffset, Held] = Map.empty,
  decided: Set[TopicPartitionOffset] = Set.empty,
  renewing: Set[TopicPartitionOffset] = Set.empty,
  deferred: Map[TopicPartitionOffset, AcknowledgeType] = Map.empty,
  lock: Option[FiniteDuration] = None,
  unpulled: Set[TopicPartitionOffset] = Set.empty,
  pollId: Long = 0L,
  stagedSince: Option[FiniteDuration] = None,
  started: Boolean = false,
  stopping: Boolean = false,
  readerClosed: Boolean = false,
  waiters: Vector[Waiter[F]] = Vector.empty[Waiter[F]]
) {

  // Held records without a decision or a renewal.
  lazy val undecided: Vector[(TopicPartitionOffset, Held)] =
    held.iterator.filterNot { case (p, _) => decided(p) || renewing(p) }.toVector

  // Whether a poll's new records are handed out.
  def admitting: Boolean = !stopping && !readerClosed && unpulled.isEmpty

  // Whether the delivery is held.
  def holds(position: TopicPartitionOffset, deliveryCount: Short): Boolean =
    held.get(position).exists(_.deliveryCount == deliveryCount)

  // Java polls once every record has a decision or a renewal, and the stream took the poll. Renewed
  // records come back only from a poll, which returns no new records while a renewal is pending.
  def canPoll: Boolean = started && allSettled && (renewing.nonEmpty || admitting)

  // Whether no held record is undecided, without building `undecided`: `decided` and `renewing`
  // are disjoint sets of held records.
  def allSettled: Boolean = held.size == decided.size + renewing.size

  // After stopping, once every record is decided and the stream took the latest poll or ended. In
  // explicit mode, an untaken poll is undecided anyway.
  def isFinished: Boolean =
    stopping && held.keySet.subsetOf(decided) && (unpulled.isEmpty || readerClosed)

  // Renewals restart their records' locks at `at`, and a user's renewal the time held.
  def withStaged(
    last: Map[TopicPartitionOffset, AcknowledgeType],
    waiting: Vector[Decision],
    at: FiniteDuration,
    byUser: Boolean
  ): ShareState[F] = {
    val (renewed, final_) = last.partition(_._2 == AcknowledgeType.Renew)
    copy(
      held = held ++ renewed
        .keySet
        .map { p =>
          p -> held(p).copy(lockedSince = at, heldSince = if (byUser) at else held(p).heldSince)
        },
      decided = decided ++ final_.keySet -- renewed.keySet,
      stagedSince = stagedSince.orElse(if (final_.isEmpty) None else Some(at)),
      renewing = renewing ++ renewed.keySet,
      deferred = deferred ++ waiting.map(d => d.position -> d.acknowledgeType)
    )
  }

  // A poll sent the staged decisions and returned renewed and new records.
  def polled(
    returned: Vector[(TopicPartitionOffset, Held)],
    admitted: Vector[(TopicPartitionOffset, Held)],
    rejected: Vector[(TopicPartitionOffset, Held)],
    reportedLock: Option[FiniteDuration],
    holdAdmitted: Boolean
  ): ShareState[F] = {
    val fresh = admitted.map(_._1) ++ rejected.map(_._1)
    // A poll with new records means no renewal is pending: those not returned failed.
    val notReturned           = renewing -- returned.map(_._1)
    val (stillRenewing, lost) =
      if (fresh.nonEmpty) (Set.empty[TopicPartitionOffset], notReturned)
      else (notReturned, Set.empty[TopicPartitionOffset])
    copy(
      // In implicit mode nothing is held; rejected records only until released.
      held = held.filterKeysStrict(stillRenewing) ++ returned ++ rejected ++
        (if (holdAdmitted) admitted else Vector.empty),
      decided = Set.empty,
      stagedSince = None,
      renewing = stillRenewing,
      // A renewed record returned as a new delivery is among the lost.
      deferred = deferred -- lost,
      lock = reportedLock,
      unpulled = if (admitted.isEmpty) unpulled -- lost else admitted.map(_._1).toSet,
      pollId = if (admitted.isEmpty) pollId else pollId + 1
    )
  }

  // The stream took the poll; the max processing time counts from now.
  def withHandedOut(id: Long, at: FiniteDuration): ShareState[F] =
    if (id != pollId) this
    else
      copy(
        held = held ++ unpulled.flatMap(p => held.get(p).map(h => p -> h.copy(heldSince = at))),
        unpulled = Set.empty
      )

  // The staged decisions were sent.
  def withoutDecided: ShareState[F] =
    copy(held = held -- decided, decided = Set.empty, stagedSince = None)

  // Completes the requests waiting for a decision on the position with its staging result, as the
  // effects to run.
  def settled(
    position: TopicPartitionOffset,
    result: Either[Throwable, Unit]
  ): (ShareState[F], Vector[F[Unit]]) = {
    val (hit, rest)   = waiters.partition(_.positions(position))
    val left          = hit.map(w => w.copy(positions = w.positions - position))
    val (ready, keep) =
      if (result.isLeft) (hit, Vector.empty) else left.partition(_.positions.isEmpty)
    (copy(waiters = rest ++ keep), ready.map(_.done(result)))
  }

  // Fails the waiting decisions whose record lost its lock before a poll returned it.
  def settledLost: (ShareState[F], Vector[F[Unit]]) =
    waiters
      .flatMap(_.positions)
      .distinct
      .filterNot(deferred.contains)
      .foldLeft((this, Vector.empty[F[Unit]])) { case ((s, effects), p) =>
        val (next, more) = s.settled(p, Left(RecordNotInFlightException(p._1, p._2)))
        (next, effects ++ more)
      }

  // The renewal failed, so the records lost their lock.
  def withoutRenewing(positions: Set[TopicPartitionOffset]): ShareState[F] = {
    val lost = positions.intersect(renewing)
    if (lost.isEmpty) this
    else
      copy(
        held = held -- lost,
        renewing = renewing -- lost,
        deferred = deferred -- lost,
        unpulled = unpulled -- lost
      )
  }

}
