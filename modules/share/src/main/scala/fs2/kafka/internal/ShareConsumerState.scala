/*
 * Copyright 2018 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka.internal

import fs2.kafka.AcknowledgeType
import fs2.kafka.KafkaByteConsumerRecord
import fs2.kafka.instances.fs2KafkaTopicPartitionOrdering
import fs2.kafka.internal.Acknowledgement.TopicPartitionOffset
import scala.collection.immutable.SortedSet
import scala.concurrent.duration.Duration
import scala.concurrent.duration.FiniteDuration

/**
  * A delivery of a record of the poll being processed.
  *
  * @param record
  *   the Java record to acknowledge it by, from the poll which last returned it
  * @param heldSince
  *   when the consumer took its poll, or the user's latest renewal; the max processing time counts
  *   from it
  */
final private[kafka] case class Delivery(
  record: KafkaByteConsumerRecord,
  deliveryCount: Short,
  heldSince: FiniteDuration
)

/**
  * When the timers next renew the unacknowledged records, send the pending acknowledgements,
  * release the first record past the max processing time, and check the renewals not yet back,
  * each if it applies.
  */
final private[kafka] case class Deadlines(
  renewal: Option[FiniteDuration],
  send: Option[FiniteDuration],
  overdue: Option[FiniteDuration],
  check: Option[FiniteDuration]
) {

  def next: Option[FiniteDuration] =
    List(renewal, send, overdue, check).flatten.reduceOption(_ min _)

}

/**
  * A record being renewed. Java takes no acknowledgement for it until a poll returned it again,
  * which it does once the broker confirmed the renewal; one made meanwhile is kept here, and given
  * to Java then.
  *
  * @param since
  *   when the renewal was sent
  * @param confirmed
  *   whether Java reported the renewal done, so that a poll returns the record
  * @param acknowledgement
  *   the last acknowledgement made meanwhile, if any
  */
final private[kafka] case class Renewing(
  delivery: Delivery,
  since: FiniteDuration,
  confirmed: Boolean,
  acknowledgement: Option[AcknowledgeType]
)

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
  * @param renewing
  *   the records being renewed, not held until a poll returns them; they outlive the poll they
  *   belong to, so that the acknowledgements made meanwhile reach Java
  * @param checkedAt
  *   when Java was last asked how the renewals went
  */
final private[kafka] case class ShareConsumerState(
  held: Map[TopicPartitionOffset, Delivery],
  acked: Set[TopicPartitionOffset],
  unsentSince: Option[FiniteDuration],
  lockedSince: FiniteDuration,
  lock: Option[FiniteDuration],
  timed: SortedSet[(FiniteDuration, TopicPartitionOffset)],
  renewing: Map[TopicPartitionOffset, Renewing],
  checkedAt: FiniteDuration
) {

  def holds(offset: TopicPartitionOffset, deliveryCount: Short): Boolean =
    held.get(offset).exists(_.deliveryCount == deliveryCount) ||
      renewing.get(offset).exists(_.delivery.deliveryCount == deliveryCount)

  def isRenewing(offset: TopicPartitionOffset): Boolean = renewing.contains(offset)

  def hasRenewing: Boolean = renewing.nonEmpty

  // Every renewal is confirmed, so a poll returns the records without fetching new ones; and none
  // is still in flight, or Java's poll would refuse.
  def canTakeBack: Boolean = renewing.nonEmpty && renewing.values.forall(_.confirmed) && !hasUnacked

  def record(offset: TopicPartitionOffset): KafkaByteConsumerRecord = held(offset).record

  def hasUnacked: Boolean = held.size > acked.size

  // Scans the held records: only for a renewal or the end of a poll, not per acknowledgement.
  def unacked: Vector[TopicPartitionOffset] = held.keysIterator.filterNot(acked).toVector

  def hasUnsent: Boolean = acked.nonEmpty

  // When automatic renewal next renews the unacknowledged records.
  def nextRenewal(interval: FiniteDuration): Option[FiniteDuration] =
    if (hasUnacked) Some(lockedSince + interval) else None

  def nextSend(delay: FiniteDuration): Option[FiniteDuration] = unsentSince.map(_ + delay)

  def nextCheck(interval: FiniteDuration): Option[FiniteDuration] =
    if (renewing.nonEmpty) Some(checkedAt + interval) else None

  def nextOverdue(max: FiniteDuration): Option[FiniteDuration] =
    timed.headOption.map { case (since, _) => since + max }

  def overdue(max: FiniteDuration, now: FiniteDuration): Vector[TopicPartitionOffset] =
    timed.iterator.takeWhile { case (since, _) => since + max <= now }.map(_._2).toVector

  private def timedEntries(offsets: Iterable[TopicPartitionOffset]) =
    offsets.flatMap(p => held.get(p).map(d => d.heldSince -> p))

  // Java recorded these acknowledgements; they are sent later.
  def withAcked(offsets: Iterable[TopicPartitionOffset], at: FiniteDuration): ShareConsumerState =
    if (offsets.isEmpty) this
    else
      copy(
        acked = acked ++ offsets,
        unsentSince = unsentSince.orElse(Some(at)),
        timed = timed -- timedEntries(offsets)
      )

  // The pending acknowledgements were sent: their records are no longer held.
  def withoutAcked: ShareConsumerState =
    copy(held = held -- acked, acked = Set.empty, unsentSince = None)

  // Java no longer holds these, so they cannot be acknowledged.
  def withoutHeld(offsets: Iterable[TopicPartitionOffset]): ShareConsumerState = {
    val pending = acked -- offsets
    copy(
      held = held -- offsets,
      acked = pending,
      unsentSince = if (pending.isEmpty) None else unsentSince,
      timed = timed -- timedEntries(offsets)
    )
  }

  // The poll's processing ended: its records can no longer be acknowledged or renewed. Those still
  // being renewed without an acknowledgement are released once back, as `process` left them.
  def closed: ShareConsumerState =
    ShareConsumerState.empty.copy(
      lock = lock,
      renewing = renewing.map { case (p, r) =>
        p -> r.copy(acknowledgement = r.acknowledgement.orElse(Some(AcknowledgeType.Release)))
      },
      checkedAt = checkedAt
    )

  /**
    * A renewal sent the pending acknowledgements and renewed `asked` at `at`, except those Java no
    * longer held, `gone`. The renewed records are not held until a poll returns them. Records the
    * user renewed replaced their earlier acknowledgement, and their time held restarts.
    */
  def withRenewing(
    asked: Set[TopicPartitionOffset],
    gone: Set[TopicPartitionOffset],
    byUser: Set[TopicPartitionOffset],
    at: FiniteDuration,
    reportedLock: Option[FiniteDuration]
  ): ShareConsumerState = {
    val sent = acked -- byUser
    val renewed = (asked -- gone).iterator.flatMap { p =>
      held.get(p).map { d =>
        p -> Renewing(if (byUser(p)) d.copy(heldSince = at) else d, at, false, None)
      }
    }
    copy(
      held = held -- sent -- asked,
      acked = Set.empty,
      unsentSince = None,
      lockedSince = at,
      lock = reportedLock.orElse(lock),
      timed = timed -- timedEntries(sent ++ asked),
      renewing = renewing ++ renewed,
      checkedAt = at
    )
  }

  // Unsubscribed, a renewal renews nothing; its time is still taken, or it would be due at once again.
  def withRenewalSkipped(at: FiniteDuration): ShareConsumerState = copy(lockedSince = at)

  // Acknowledgements made while renewing, kept until the records are back; a renewal clears one.
  def withKept(acks: Vector[(TopicPartitionOffset, AcknowledgeType)]): ShareConsumerState =
    copy(renewing = acks.foldLeft(renewing) { case (r, (p, kind)) =>
      r.get(p).fold(r) { entry =>
        r.updated(
          p,
          entry.copy(acknowledgement = if (kind == AcknowledgeType.Renew) None else Some(kind))
        )
      }
    })

  /**
    * What Java reported of the sent acknowledgements at `at`: a confirmed renewal waits for the
    * poll which returns its record, a failed one loses the record, which is returned. Renewals
    * older than `forgetBefore` can't be reported anymore, and are forgotten.
    */
  def withResults(
    results: Vector[(TopicPartitionOffset, Boolean)],
    at: FiniteDuration,
    forgetBefore: FiniteDuration
  ): (ShareConsumerState, Set[TopicPartitionOffset]) = {
    val reported = results.filter { case (p, _) => renewing.get(p).exists(!_.confirmed) }
    val failed = reported.collect { case (p, false) => p }.toSet
    val confirmed = reported.collect { case (p, true) => p }.toSet -- failed
    val next = (renewing -- failed).map { case (p, r) =>
      p -> (if (confirmed(p)) r.copy(confirmed = true) else r)
    }
    val forgotten = next.filter { case (_, r) => r.since < forgetBefore }.keySet
    (copy(renewing = next -- forgotten, checkedAt = at), failed ++ forgotten)
  }

  /**
    * A poll returned renewed records: they are held again, and the acknowledgements made meanwhile
    * are returned, to give to Java. Records it returned which this state doesn't know follow.
    */
  def withTakenBack(
    returned: Vector[(TopicPartitionOffset, KafkaByteConsumerRecord)],
    reportedLock: Option[FiniteDuration]
  ): (
    ShareConsumerState,
    (Vector[(TopicPartitionOffset, AcknowledgeType)], Vector[KafkaByteConsumerRecord])
  ) = {
    val (back, unknown) = returned.partition { case (p, _) => renewing.contains(p) }
    val restored = back.map { case (p, r) =>
      p -> renewing(p).copy(delivery = renewing(p).delivery.copy(record = r))
    }
    val acks = restored.flatMap { case (p, r) => r.acknowledgement.map(p -> _) }
    val unacked = restored.collect { case (p, r) if r.acknowledgement.isEmpty => r.delivery.heldSince -> p }
    (
      copy(
        held = held ++ restored.map { case (p, r) => p -> r.delivery },
        lock = reportedLock.orElse(lock),
        timed = timed ++ unacked,
        renewing = renewing -- back.map(_._1)
      ),
      (acks, unknown.filterNot { case (p, _) => held.contains(p) }.map(_._2))
    )
  }

  // Unsubscribed: no poll can return the records being renewed.
  def withoutRenewing: ShareConsumerState = copy(renewing = Map.empty)

}

private[kafka] object ShareConsumerState {

  // Nothing held, before the first poll.
  val empty: ShareConsumerState =
    ShareConsumerState(
      Map.empty,
      Set.empty,
      None,
      Duration.Zero,
      None,
      SortedSet.empty,
      Map.empty,
      Duration.Zero
    )

  // A new poll, started at `polledAt`. Records still being renewed are kept.
  def polled(
    deliveries: Vector[(TopicPartitionOffset, Delivery)],
    polledAt: FiniteDuration,
    reportedLock: Option[FiniteDuration],
    previous: ShareConsumerState
  ): ShareConsumerState =
    ShareConsumerState(
      held = deliveries.toMap,
      acked = Set.empty,
      unsentSince = None,
      lockedSince = polledAt,
      lock = reportedLock.orElse(previous.lock),
      timed = SortedSet.empty[(FiniteDuration, TopicPartitionOffset)] ++ deliveries.map { case (p, d) =>
        d.heldSince -> p
      },
      renewing = previous.renewing,
      checkedAt = previous.checkedAt
    )

}
