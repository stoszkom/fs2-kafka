/*
 * Copyright 2018 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka.internal

import fs2.Chunk
import fs2.kafka.AcknowledgeType
import fs2.kafka.KafkaByteConsumerRecord
import fs2.kafka.instances.fs2KafkaTopicPartitionOrdering
import fs2.kafka.internal.Acknowledgement.TopicPartitionOffset
import fs2.kafka.internal.ShareConsumerState.*
import scala.collection.immutable.SortedSet
import scala.concurrent.duration.Duration
import scala.concurrent.duration.FiniteDuration

/**
  * A delivery of a record Java holds for the consumer.
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
  * The records Java holds for the consumer, each with its status. Acknowledgements are recorded
  * here, and the consumer gives them to Java when it sends them. Every change of status goes
  * through `updated`, which keeps the counts and the max processing time's index in step, so that
  * the deadlines never need a scan.
  *
  * @param open
  *   whether `process` has the poll: once it returned, its records can't be acknowledged anymore
  * @param unacked
  *   how many records have no acknowledgement, a renewal by the user included
  * @param unsent
  *   the records with an acknowledgement to send
  * @param renewals
  *   how many records the user renewed, which the next renewal takes
  * @param unsentSince
  *   when the oldest acknowledgement to send was recorded
  * @param timed
  *   the unacknowledged records by `heldSince`
  * @param lockedSince
  *   the estimated start of the unacknowledged records' lock: the poll's start or the latest
  *   renewal
  * @param lock
  *   the acquisition lock last reported by the broker
  */
final private[kafka] case class ShareConsumerState(
  open: Boolean,
  records: Map[TopicPartitionOffset, Entry],
  unacked: Int,
  unsent: Set[TopicPartitionOffset],
  renewals: Int,
  unsentSince: Option[FiniteDuration],
  timed: SortedSet[(FiniteDuration, TopicPartitionOffset)],
  lockedSince: FiniteDuration,
  lock: Option[FiniteDuration]
) {

  def hasRenewing: Boolean = records.size > unacked + unsent.size

  def holds(offset: TopicPartitionOffset, deliveryCount: Short): Boolean =
    open && records.get(offset).exists(_.delivery.deliveryCount == deliveryCount)

  // A renewal the user asked for is due at once.
  def nextRenewal(interval: Option[FiniteDuration]): Option[FiniteDuration] =
    if (renewals > 0) Some(Duration.Zero)
    else interval.filter(_ => unacked > 0).map(lockedSince + _)

  // While paused no poll follows, so what is unsent is due at once. Once the poll closed, the next
  // poll, or the final commit, sends it.
  def nextSend(delay: Option[FiniteDuration], paused: Boolean): Option[FiniteDuration] =
    unsentSince.flatMap(since => if (paused) Some(since) else if (open) delay.map(since + _) else None)

  def nextOverdue(max: FiniteDuration): Option[FiniteDuration] =
    timed.headOption.map { case (since, _) => since + max }

  // The one change of a record's entry, which keeps the counts and the index in step.
  private def updated(
    offset: TopicPartitionOffset,
    next: Option[Entry],
    at: FiniteDuration
  ): ShareConsumerState = {
    val previous = records.get(offset)
    val unsentNow = if (next.exists(_.unsent > 0)) unsent + offset else unsent - offset
    copy(
      records = next.fold(records - offset)(records.updated(offset, _)),
      unacked = unacked + previous.fold(0)(-_.unacked) + next.fold(0)(_.unacked),
      unsent = unsentNow,
      renewals = renewals + previous.fold(0)(-_.renewal) + next.fold(0)(_.renewal),
      unsentSince =
        if (unsentNow.isEmpty) None
        else if (unsentNow.size > unsent.size) unsentSince.orElse(Some(at))
        else unsentSince,
      timed = timed -- previous.flatMap(_.timed(offset)) ++ next.flatMap(_.timed(offset))
    )
  }

  private def withStatuses(
    changes: Iterable[(TopicPartitionOffset, Option[Entry])],
    at: FiniteDuration
  ): ShareConsumerState =
    changes.foldLeft(this) { case (s, (p, next)) => s.updated(p, next, at) }

  private def withStatus(
    offsets: Iterable[TopicPartitionOffset],
    at: FiniteDuration
  )(status: Entry => Option[Entry]): ShareConsumerState =
    withStatuses(offsets.flatMap(p => records.get(p).map(e => p -> status(e))), at)

  /**
    * Records a chunk's acknowledgements at `at`, the last per record winning; one for a record being
    * renewed waits for it, a renewal too, which then comes after it. Returns the offsets whose
    * delivery isn't held, in order.
    */
  def acknowledged(
    acks: Chunk[Acknowledgement],
    at: FiniteDuration
  ): (ShareConsumerState, Vector[TopicPartitionOffset]) = {
    val (inFlight, notHeld) = acks.toVector.partition(a => holds(a.offset, a.deliveryCount))
    val next = inFlight.foldLeft(this) { (s, a) =>
      s.withStatus(List(a.offset), at) { e =>
        Some(e.copy(status = e.status match {
          case Renewing(_) => Renewing(Some(a.acknowledgeType))
          case _ => Acked(a.acknowledgeType)
        }))
      }
    }
    (next, notHeld.map(_.offset))
  }

  // Takes the acknowledgements to send, to give to Java: their records are no longer held.
  def takeUnsent: (ShareConsumerState, Vector[(KafkaByteConsumerRecord, AcknowledgeType)]) = {
    val sent = unsent.toVector.flatMap(p => records.get(p).map(p -> _))
    (
      withStatuses(sent.map { case (p, _) => p -> None }, lockedSince),
      sent.collect { case (_, Entry(d, Acked(kind))) => d.record -> kind }
    )
  }

  // The records without an acknowledgement for `max` are released.
  def withOverdueReleased(max: FiniteDuration, now: FiniteDuration): ShareConsumerState =
    withStatus(timed.iterator.takeWhile { case (since, _) => since + max <= now }.map(_._2).toVector, now)(
      e => Some(e.copy(status = Acked(AcknowledgeType.Release)))
    )

  // `process` returned at `at`: its records can no longer be acknowledged, and those it left are
  // released, a renewal by the user included. Records being renewed are released once back.
  def closed(at: FiniteDuration): ShareConsumerState =
    copy(open = false).withStatus(records.collect { case (p, e) if e.unacked > 0 => p }, at)(e =>
      Some(e.copy(status = Acked(AcknowledgeType.Release)))
    )

  /**
    * A renewal at `at` of every unacknowledged record, those the user renewed included, whose time
    * held restarts. Returns the records it renews: they are no longer held until a poll returns
    * them, and acknowledgements made meanwhile wait for them.
    */
  def renewal(
    at: FiniteDuration
  ): (ShareConsumerState, Vector[(TopicPartitionOffset, KafkaByteConsumerRecord)]) = {
    val asked = records.toVector.collect { case (p, e) if e.unacked > 0 => p -> e }
    val renewed = asked.map { case (p, e) =>
      val delivery = if (e.renewal > 0) e.delivery.copy(heldSince = at) else e.delivery
      p -> Entry(delivery, Renewing(None))
    }
    (
      withStatuses(renewed.map { case (p, e) => p -> Some(e) }, at).copy(lockedSince = at),
      renewed.map { case (p, e) => p -> e.delivery.record }
    )
  }

  // Unsubscribed, a renewal renews nothing: the user's renewals are dropped, and the time is still
  // taken, or it would be due at once again.
  def withRenewalSkipped(at: FiniteDuration): ShareConsumerState =
    withStatus(records.collect { case (p, e) if e.renewal > 0 => p }, at)(e => Some(e.copy(status = Unacked)))
      .copy(lockedSince = at)

  /**
    * A renewal ended at `at`: a poll returned the records whose renewal the broker confirmed, which
    * are held again with the acknowledgement made meanwhile, or released if the poll closed; the
    * others are lost. Returns the records the poll returned which this state doesn't know.
    */
  def withRenewalEnded(
    returned: Vector[KafkaByteConsumerRecord],
    reportedLock: Option[FiniteDuration],
    at: FiniteDuration
  ): (ShareConsumerState, Vector[KafkaByteConsumerRecord]) = {
    val byOffset = returned.map(r => offsetOf(r) -> r).toMap
    val ended = records.toVector.collect { case (p, Entry(d, Renewing(kept))) =>
      p -> byOffset.get(p).map { record =>
        val status =
          if (open) kept.fold[Status](Unacked)(Acked(_))
          else Acked(kept.filter(_ != AcknowledgeType.Renew).getOrElse(AcknowledgeType.Release))
        Entry(d.copy(record = record), status)
      }
    }
    (
      withStatuses(ended, at).copy(lock = reportedLock.orElse(lock)),
      returned.filterNot(r => records.contains(offsetOf(r)))
    )
  }

  // A renewal failed half-way: the records being renewed are held again as they were.
  def withRenewalFailed(at: FiniteDuration): ShareConsumerState =
    withRenewalEnded(records.values.collect { case Entry(d, Renewing(_)) => d.record }.toVector, None, at)._1

}

private[kafka] object ShareConsumerState {

  /**
    * What the consumer knows of a record: unacknowledged, acknowledged but not yet given to Java,
    * a `Renew` being an acknowledgement the next renewal takes, or being renewed, with the
    * acknowledgement made meanwhile.
    */
  sealed abstract class Status
  case object Unacked extends Status
  final case class Acked(kind: AcknowledgeType) extends Status
  final case class Renewing(kept: Option[AcknowledgeType]) extends Status

  final case class Entry(delivery: Delivery, status: Status) {

    def unacked: Int =
      status match {
        case Unacked | Acked(AcknowledgeType.Renew) => 1
        case _ => 0
      }

    def unsent: Int =
      status match {
        case Acked(kind) if kind != AcknowledgeType.Renew => 1
        case _ => 0
      }

    def renewal: Int =
      status match {
        case Acked(AcknowledgeType.Renew) => 1
        case _ => 0
      }

    // A record the user renewed is not timed: its time held restarts with the renewal.
    def timed(offset: TopicPartitionOffset): Option[(FiniteDuration, TopicPartitionOffset)] =
      if (status == Unacked) Some(delivery.heldSince -> offset) else None

  }

  def offsetOf(record: KafkaByteConsumerRecord): TopicPartitionOffset =
    new org.apache.kafka.common.TopicPartition(record.topic, record.partition) -> record.offset

  // Nothing held, before the first poll.
  val empty: ShareConsumerState =
    ShareConsumerState(false, Map.empty, 0, Set.empty, 0, None, SortedSet.empty, Duration.Zero, None)

  // A new poll, started at `polledAt`, with the lock last reported. Nothing was held before it.
  def polled(
    deliveries: Vector[(TopicPartitionOffset, Delivery)],
    polledAt: FiniteDuration,
    lock: Option[FiniteDuration]
  ): ShareConsumerState =
    empty
      .copy(open = true, lockedSince = polledAt, lock = lock)
      .withStatuses(deliveries.map { case (p, d) => p -> Some(Entry(d, Unacked)) }, polledAt)

}
