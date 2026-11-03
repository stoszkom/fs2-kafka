/*
 * Copyright 2018 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka.sync

import fs2.kafka.KafkaByteConsumerRecord
import fs2.kafka.instances.fs2KafkaTopicPartitionOrdering
import fs2.kafka.internal.actor.TopicPartitionOffset
import scala.collection.immutable.SortedSet
import scala.concurrent.duration.Duration
import scala.concurrent.duration.FiniteDuration

/**
  * A delivery of a record of the poll being processed.
  *
  * @param record
  *   the Java record to acknowledge it by, from the poll which last returned it
  * @param heldSince
  *   when `process` got it, or the user's latest renewal; the max processing time counts from it
  */
final private[sync] case class Delivery(
  record: KafkaByteConsumerRecord,
  deliveryCount: Short,
  heldSince: FiniteDuration
)

/**
  * When the timers next renew the unacknowledged records, send the pending acknowledgements, and
  * release the first record past the max processing time, each if it applies.
  */
final private[sync] case class Deadlines(
  renewal: Option[FiniteDuration],
  send: Option[FiniteDuration],
  overdue: Option[FiniteDuration]
) {

  def next: Option[FiniteDuration] = List(renewal, send, overdue).flatten.reduceOption(_ min _)

}

/**
  * The records of the poll being processed. Only one poll is processed at a time, and a renewal
  * renews every unacknowledged record at once, so they all share one lock start.
  *
  * @param held
  *   deliveries whose acknowledgement Java has not sent
  * @param acked
  *   held deliveries with an acknowledgement recorded in Java but not yet sent
  * @param unsentSince
  *   when the oldest unsent acknowledgement was recorded
  * @param lockedSince
  *   the estimated start of the unacknowledged records' lock: the poll's start or the latest
  *   renewal
  * @param lock
  *   the acquisition lock last reported by the broker
  * @param timed
  *   the unacknowledged records by `heldSince`, so that the max processing time finds the earliest
  *   without scanning
  */
final private[sync] case class SyncState(
  held: Map[TopicPartitionOffset, Delivery],
  acked: Set[TopicPartitionOffset],
  unsentSince: Option[FiniteDuration],
  lockedSince: FiniteDuration,
  lock: Option[FiniteDuration],
  timed: SortedSet[(FiniteDuration, TopicPartitionOffset)]
) {

  def holds(offset: TopicPartitionOffset, deliveryCount: Short): Boolean =
    held.get(offset).exists(_.deliveryCount == deliveryCount)

  def record(offset: TopicPartitionOffset): KafkaByteConsumerRecord = held(offset).record

  def withLock(reported: Option[FiniteDuration]): SyncState = copy(lock = reported.orElse(lock))

  def hasUnacked: Boolean = held.size > acked.size

  // Scans the held records: only for a renewal or the end of a poll, not per acknowledgement.
  def unacked: Vector[TopicPartitionOffset] = held.keysIterator.filterNot(acked).toVector

  def hasUnsent: Boolean = acked.nonEmpty

  // When automatic renewal next renews the unacknowledged records.
  def nextRenewal(interval: FiniteDuration): Option[FiniteDuration] =
    if (hasUnacked) Some(lockedSince + interval) else None

  def nextSend(delay: FiniteDuration): Option[FiniteDuration] = unsentSince.map(_ + delay)

  def nextOverdue(max: FiniteDuration): Option[FiniteDuration] =
    timed.headOption.map { case (since, _) => since + max }

  def overdue(max: FiniteDuration, now: FiniteDuration): Vector[TopicPartitionOffset] =
    timed.iterator.takeWhile { case (since, _) => since + max <= now }.map(_._2).toVector

  private def timedEntries(offsets: Iterable[TopicPartitionOffset]) =
    offsets.flatMap(p => held.get(p).map(d => d.heldSince -> p))

  // Java recorded these acknowledgements; they are sent later.
  def withAcked(offsets: Iterable[TopicPartitionOffset], at: FiniteDuration): SyncState =
    if (offsets.isEmpty) this
    else
      copy(
        acked = acked ++ offsets,
        unsentSince = unsentSince.orElse(Some(at)),
        timed = timed -- timedEntries(offsets)
      )

  // The pending acknowledgements were sent: their records are no longer held.
  def withoutAcked: SyncState =
    copy(held = held -- acked, acked = Set.empty, unsentSince = None)

  // Java no longer holds these, so they cannot be acknowledged.
  def withoutHeld(offsets: Iterable[TopicPartitionOffset]): SyncState =
    copy(held = held -- offsets, acked = acked -- offsets, timed = timed -- timedEntries(offsets))

  // The poll's processing ended: its records can no longer be acknowledged or renewed.
  def closed: SyncState = SyncState.empty.withLock(lock)

  /**
    * A renewal sent the pending acknowledgements and renewed the unacknowledged records at `at`.
    * Those a poll returned are held again; the rest lost their lock. Records the user renewed
    * replaced their earlier acknowledgement, and their time held restarts.
    */
  def withRenewed(
    returned: Map[TopicPartitionOffset, KafkaByteConsumerRecord],
    byUser: Set[TopicPartitionOffset],
    at: FiniteDuration,
    reportedLock: Option[FiniteDuration]
  ): SyncState = {
    val sent = acked -- byUser
    val lost = held.keySet -- sent -- returned.keySet
    val kept = returned.map { case (p, r) =>
      val old = held(p)
      p -> old.copy(record = r, heldSince = if (byUser(p)) at else old.heldSince)
    }
    val reset = byUser.intersect(returned.keySet)
    SyncState(
      held = kept,
      acked = Set.empty,
      unsentSince = None,
      lockedSince = at,
      lock = reportedLock.orElse(lock),
      timed = timed -- timedEntries(lost ++ byUser) ++ reset.map(at -> _)
    )
  }

}

private[sync] object SyncState {

  // Nothing held, before the first poll.
  val empty: SyncState =
    SyncState(Map.empty, Set.empty, None, Duration.Zero, None, SortedSet.empty)

  // A new poll, started at `polledAt`, which `process` got at `at`.
  def polled(
    deliveries: Vector[(TopicPartitionOffset, Delivery)],
    polledAt: FiniteDuration,
    reportedLock: Option[FiniteDuration],
    previous: SyncState
  ): SyncState =
    SyncState(
      held = deliveries.toMap,
      acked = Set.empty,
      unsentSince = None,
      lockedSince = polledAt,
      lock = reportedLock.orElse(previous.lock),
      timed = SortedSet.empty[(FiniteDuration, TopicPartitionOffset)] ++ deliveries.map { case (p, d) =>
        d.heldSince -> p
      }
    )

}
