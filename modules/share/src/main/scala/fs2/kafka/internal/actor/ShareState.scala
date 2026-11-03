/*
 * Copyright 2018 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka.internal.actor

import fs2.kafka.AcknowledgeType
import fs2.kafka.KafkaByteConsumerRecord
import fs2.kafka.RecordNotInFlightException
import fs2.kafka.instances.fs2KafkaTopicPartitionOrdering
import fs2.kafka.internal.syntax.*
import scala.collection.immutable.SortedSet
import scala.concurrent.duration.FiniteDuration

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

// A request completed once Java recorded its acknowledgements for renewing records, by delivery, so that a
// new delivery of the same offset does not complete it.
final private[kafka] case class Waiter[F[_]](
  deliveries: Set[(TopicPartitionOffset, Short)],
  done: Either[Throwable, Unit] => F[Unit]
)

/**
  * @param held
  *   records whose acknowledgement Java has not sent, and renewed records not returned yet
  * @param acked
  *   held records with an acknowledgement recorded in Java but not yet sent
  * @param renewing
  *   held records whose renewal was sent, which a later poll returns
  * @param deferred
  *   acknowledgements for renewing records, given to Java once a poll returns them
  * @param lock
  *   the acquisition lock last reported by the broker
  * @param unpulled
  *   the latest poll's records while the stream has not taken it
  * @param pollId
  *   the latest poll offered to the stream, to tell a late hand-out of an earlier one apart
  * @param unsentSince
  *   when the oldest unsent acknowledgement was recorded
  * @param waiters
  *   requests completed once Java recorded their acknowledgements in `deferred`
  * @param paused
  *   whether the pause signal holds back polls for new records
  * @param acceptPending
  *   in implicit mode, whether the stream took a poll which no poll or commit accepted yet
  * @param implicitMode
  *   whether Java accepts each poll itself, so that new records are not held
  * @param timed
  *   the unacknowledged records the stream took, which are neither renewing nor in an untaken poll,
  *   by when they were taken or last renewed by the user, so that the max processing time finds the
  *   earliest without scanning the held records
  */
final private[kafka] case class ShareState[F[_]](
  held: Map[TopicPartitionOffset, Held] = Map.empty,
  acked: Set[TopicPartitionOffset] = Set.empty,
  renewing: Set[TopicPartitionOffset] = Set.empty,
  deferred: Map[TopicPartitionOffset, AcknowledgeType] = Map.empty,
  lock: Option[FiniteDuration] = None,
  unpulled: Set[TopicPartitionOffset] = Set.empty,
  pollId: Long = 0L,
  unsentSince: Option[FiniteDuration] = None,
  started: Boolean = false,
  stopping: Boolean = false,
  readerClosed: Boolean = false,
  waiters: Vector[Waiter[F]] = Vector.empty[Waiter[F]],
  paused: Boolean = false,
  acceptPending: Boolean = false,
  implicitMode: Boolean = false,
  timed: SortedSet[(FiniteDuration, TopicPartitionOffset)] =
    SortedSet.empty[(FiniteDuration, TopicPartitionOffset)]
) {

  // Held records without an acknowledgement or a renewal.
  lazy val unacked: Vector[(TopicPartitionOffset, Held)] =
    held.iterator.filterNot { case (p, _) => acked(p) || renewing(p) }.toVector

  // Whether a poll's new records are handed out, and a poll may acquire them. New records a renewal poll
  // returns while paused are released.
  def admitting: Boolean = !stopping && !readerClosed && !paused && unpulled.isEmpty

  // The records the max processing time releases at `now`.
  def overdue(max: FiniteDuration, now: FiniteDuration): Vector[TopicPartitionOffset] =
    timed.iterator.takeWhile { case (since, _) => since + max <= now }.map(_._2).toVector

  // When the max processing time next releases a record.
  def nextOverdue(max: FiniteDuration): Option[FiniteDuration] =
    timed.headOption.map { case (since, _) => since + max }

  private def timedEntries(offsets: Iterable[TopicPartitionOffset]) =
    offsets.flatMap(p => held.get(p).map(h => h.heldSince -> p))

  // The records acquired and not yet acknowledged: in implicit mode, the untaken poll is not held.
  def inFlight: Int = held.size + (if (implicitMode) unpulled.size else 0)

  // Whether the delivery is held.
  def holds(offset: TopicPartitionOffset, deliveryCount: Short): Boolean =
    held.get(offset).exists(_.deliveryCount == deliveryCount)

  // Java polls once every record has an acknowledgement or a renewal, and the stream took the poll. Renewed
  // records come back only from a poll; one made once their renewal completed fetches no new records.
  def canPoll: Boolean = started && allSettled && (renewing.nonEmpty || admitting)

  // Whether every held record is acked or renewing. `acked` and `renewing` are disjoint sets of held
  // records, so no need to build `unacked`.
  def allSettled: Boolean = held.size == acked.size + renewing.size

  // After stopping, once every record is acknowledged and the stream took the latest poll or ended. In
  // explicit mode, an untaken poll is unacknowledged anyway.
  def isFinished: Boolean =
    stopping && held.keySet.subsetOf(acked) && (unpulled.isEmpty || readerClosed)

  def withStarted: ShareState[F] = copy(started = true)

  def withStopping: ShareState[F] = copy(stopping = true)

  def withPaused(paused: Boolean): ShareState[F] = copy(paused = paused)

  def withReaderClosed: ShareState[F] = copy(readerClosed = true)

  def withWaiters(more: Vector[Waiter[F]]): ShareState[F] = copy(waiters = waiters ++ more)

  // Acknowledgements given to Java once a poll returned their records.
  def withoutDeferred(offsets: Vector[TopicPartitionOffset]): ShareState[F] =
    copy(deferred = deferred -- offsets)

  // The stream will not take the latest poll: renewing records of it are released once a poll returns
  // them; the unacknowledged rest are returned, to release now.
  def withoutUnpulled: (ShareState[F], Vector[TopicPartitionOffset]) = {
    val unackedOffsets = unacked.map(_._1).toSet
    (
      copy(
        unpulled = Set.empty,
        deferred = deferred ++ unpulled.filter(renewing).map(_ -> AcknowledgeType.Release),
        timed = timed ++ timedEntries(unpulled.filter(unackedOffsets))
      ),
      unpulled.filter(unackedOffsets).toVector
    )
  }

  // Renewals restart their records' locks at `at`, and a user's renewal the time held. Records Java no
  // longer holds count as acknowledged, so that they are dropped once sent.
  def withAcked(
    last: Map[TopicPartitionOffset, AcknowledgeType],
    gone: Set[TopicPartitionOffset],
    waiting: Vector[Acknowledgement],
    at: FiniteDuration,
    byUser: Boolean
  ): ShareState[F] = {
    val (renewed, final_) = last.partition(_._2 == AcknowledgeType.Renew)
    copy(
      timed = timed -- timedEntries(last.keySet ++ gone),
      held = held ++ renewed.keySet
        .map { p =>
          p -> held(p).copy(lockedSince = at, heldSince = if (byUser) at else held(p).heldSince)
        },
      acked = acked ++ final_.keySet -- renewed.keySet ++ gone,
      unsentSince = unsentSince.orElse(if (final_.isEmpty) None else Some(at)),
      renewing = renewing ++ renewed.keySet,
      deferred = deferred ++ waiting.map(a => a.offset -> a.acknowledgeType)
    )
  }

  // A poll sent the pending acknowledgements and returned renewed and new records. A renewed record's
  // lock runs from when its renewal was sent.
  def polled(
    returnedRecords: Vector[(TopicPartitionOffset, KafkaByteConsumerRecord)],
    admitted: Vector[(TopicPartitionOffset, Held)],
    rejected: Vector[(TopicPartitionOffset, Held)],
    reportedLock: Option[FiniteDuration]
  ): ShareState[F] = {
    val returned = returnedRecords.map { case (p, r) => p -> held(p).copy(record = r) }
    val fresh = admitted.map(_._1) ++ rejected.map(_._1)
    // A poll with new records means no renewal is pending: those not returned failed.
    val notReturned = renewing -- returned.map(_._1)
    val (stillRenewing, lost) =
      if (fresh.nonEmpty) (Set.empty[TopicPartitionOffset], notReturned)
      else (notReturned, Set.empty[TopicPartitionOffset])
    val nextUnpulled = if (admitted.isEmpty) unpulled -- lost else admitted.map(_._1).toSet
    val newlyHeld = returned ++ rejected ++ (if (implicitMode) Vector.empty else admitted)
    copy(
      // Only the records of this poll are held besides the renewing ones, which are not timed.
      timed = SortedSet.empty[(FiniteDuration, TopicPartitionOffset)] ++ newlyHeld.collect {
        case (p, h) if !nextUnpulled(p) => h.heldSince -> p
      },
      // In implicit mode nothing is held; rejected records only until released.
      held = held.filterKeysStrict(stillRenewing) ++ returned ++ rejected ++
        (if (implicitMode) Vector.empty else admitted),
      acked = Set.empty,
      unsentSince = None,
      renewing = stillRenewing,
      // A renewed record returned as a new delivery is among the lost.
      deferred = deferred -- lost,
      lock = reportedLock,
      acceptPending = false,
      unpulled = nextUnpulled,
      pollId = if (admitted.isEmpty) pollId else pollId + 1
    )
  }

  // The stream took the poll; the max processing time counts from now. In implicit mode, the next poll
  // or commit accepts it.
  def withHandedOut(id: Long, at: FiniteDuration): ShareState[F] =
    if (id != pollId) this
    else
      copy(
        held = held ++ unpulled.flatMap(p => held.get(p).map(h => p -> h.copy(heldSince = at))),
        timed = timed ++ unpulled.filter(p => held.contains(p) && !acked(p) && !renewing(p)).map(at -> _),
        unpulled = Set.empty,
        acceptPending = acceptPending || implicitMode
      )

  // The pending acknowledgements were sent.
  def withoutAcked: ShareState[F] =
    copy(held = held -- acked, acked = Set.empty, unsentSince = None, acceptPending = false)

  // Completes the requests waiting for an acknowledgement on the delivery with its result, as the
  // effects to run.
  def settled(
    offset: TopicPartitionOffset,
    deliveryCount: Short,
    result: Either[Throwable, Unit]
  ): (ShareState[F], Vector[F[Unit]]) = {
    val delivery = offset -> deliveryCount
    val (hit, rest) = waiters.partition(_.deliveries(delivery))
    val left = hit.map(w => w.copy(deliveries = w.deliveries - delivery))
    val (ready, keep) =
      if (result.isLeft) (hit, Vector.empty) else left.partition(_.deliveries.isEmpty)
    (copy(waiters = rest ++ keep), ready.map(_.done(result)))
  }

  // Fails the waiting acknowledgements whose delivery lost its lock before a poll returned it, also when
  // a new delivery of the record is held and waits again.
  def settledLost: (ShareState[F], Vector[F[Unit]]) =
    waiters
      .flatMap(_.deliveries)
      .distinct
      .filterNot { case (p, count) => deferred.contains(p) && holds(p, count) }
      .foldLeft((this, Vector.empty[F[Unit]])) { case ((s, effects), (p, count)) =>
        val (next, more) = s.settled(p, count, Left(RecordNotInFlightException(p._1, p._2)))
        (next, effects ++ more)
      }

  // The renewal failed, so the records lost their lock.
  def withoutRenewing(offsets: Set[TopicPartitionOffset]): ShareState[F] = {
    val lost = offsets.intersect(renewing)
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

private[kafka] object ShareState {

  def initial[F[_]](implicitMode: Boolean, paused: Boolean): ShareState[F] =
    ShareState[F](implicitMode = implicitMode, paused = paused)

}
