/*
 * Copyright 2018 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka.internal.actor

import scala.concurrent.duration.FiniteDuration

import cats.effect.Ref
import fs2.kafka.KafkaByteConsumerRecord

import org.apache.kafka.common.TopicPartition

private[kafka] object ShareState {

  type Id = (TopicPartition, Long)

  def empty[F[_]](now: FiniteDuration, renewAfter: FiniteDuration): ShareState[F] =
    ShareState[F](
      inFlight = Map.empty,
      commits = Vector.empty,
      deferred = Vector.empty,
      started = false,
      stopping = false,
      readerClosed = false,
      lock = None,
      renewAfter = renewAfter,
      lastPollStart = now,
      lastPollRecords = 0,
      lastAcquiredAt = now,
      confirmationTimes = Vector.empty
    )

}

/**
  * A record from the latest poll which has no acknowledgement committed yet.
  *
  * @param emitted
  *   whether the record stream handed out the record; `None` for records which are released without
  *   being handed out
  * @param renewing
  *   whether the record was renewed, so it can be acknowledged again only after the next poll
  * @param renewalConfirmed
  *   whether the broker confirmed the renewal; a renewal which timed out may still complete
  * @param lockedSince
  *   a conservative start time of the record's current acquisition lock
  */
final private[kafka] case class InFlightRecord[F[_]](
  record: KafkaByteConsumerRecord,
  deliveryCount: Short,
  emitted: Option[Ref[F, Boolean]],
  renewing: Boolean,
  lockedSince: FiniteDuration,
  renewalConfirmed: Boolean = true
)

/**
  * The state of a [[KafkaShareConsumerActor]].
  *
  * @param commits
  *   commit requests received but not yet processed
  * @param deferred
  *   commit requests for renewing records, processed again after the next poll
  * @param lock
  *   the acquisition lock duration last reported by the broker
  * @param renewAfter
  *   the lock age at which records without an acknowledgement are renewed
  * @param lastPollRecords
  *   how many records the previous poll returned, which shows whether it drained Java's buffer
  * @param lastAcquiredAt
  *   a conservative acquisition time of the records the previous poll returned
  * @param confirmationTimes
  *   recent times from submitting a commit to its confirmation
  * @param renewedLast
  *   whether the latest round was a renewal, so waiting commits go next
  */
final private[kafka] case class ShareState[F[_]](
  inFlight: Map[ShareState.Id, InFlightRecord[F]],
  commits: Vector[ShareRequest.Commit[F]],
  deferred: Vector[ShareRequest.Commit[F]],
  started: Boolean,
  stopping: Boolean,
  readerClosed: Boolean,
  lock: Option[FiniteDuration],
  renewAfter: FiniteDuration,
  lastPollStart: FiniteDuration,
  lastPollRecords: Int,
  lastAcquiredAt: FiniteDuration,
  confirmationTimes: Vector[FiniteDuration],
  renewedLast: Boolean = false
) {

  // New records are handed out only until consuming stops or the record stream ends.
  def admitting: Boolean = !stopping && !readerClosed

  // Records which Java requires to be acknowledged before the next poll.
  def unacknowledged: Map[ShareState.Id, InFlightRecord[F]] =
    inFlight.filter { case (_, record) => !record.renewing }

  def canPoll: Boolean =
    started && commits.isEmpty && unacknowledged.isEmpty && (admitting || inFlight.nonEmpty)

  def isFinished: Boolean =
    !admitting && inFlight.isEmpty && commits.isEmpty && deferred.isEmpty

  // Records from the latest poll for which no acknowledgement was requested yet.
  def pending: Int = {
    val requested = deferred.iterator.flatMap(_.acknowledgements.iterator.map(_.id)).toSet
    inFlight.count { case (id, record) => !record.renewing || !requested.contains(id) }
  }

  // Whether a decision waits for a record whose renewal the broker confirmed, so the next
  // poll returns it once every other record is acknowledged.
  def awaitsConfirmedRenewal: Boolean =
    deferred.exists(
      _.acknowledgements.exists(ack => inFlight.get(ack.id).exists(_.renewalConfirmed))
    )

  def nextRenewal: Option[FiniteDuration] = {
    val locked = unacknowledged.valuesIterator.map(_.lockedSince)
    if (locked.isEmpty) None else Some(locked.min + renewAfter)
  }

  def withStarted: ShareState[F] = copy(started = true)

  def withStopping: ShareState[F] = copy(stopping = true)

  def withReaderClosed: ShareState[F] = copy(readerClosed = true)

  def withCommit(commit: ShareRequest.Commit[F]): ShareState[F] =
    copy(commits = commits :+ commit)

  def withConfirmationTime(time: FiniteDuration): ShareState[F] =
    copy(confirmationTimes = (confirmationTimes :+ time).takeRight(8))

  override def toString: String =
    s"ShareState(inFlight = ${inFlight.size}, commits = ${commits.size}, deferred = ${deferred.size}, started = $started, stopping = $stopping, readerClosed = $readerClosed, lock = $lock, renewAfter = $renewAfter)"

}
