/*
 * Copyright 2018 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka.internal.actor

import cats.effect.IO
import fs2.kafka.AcknowledgeType
import fs2.kafka.BaseSpec
import fs2.kafka.KafkaByteConsumerRecord
import fs2.kafka.instances.fs2KafkaTopicPartitionOrdering
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import scala.collection.immutable.SortedSet
import scala.concurrent.duration.*

final class ShareStateSpec extends BaseSpec {

  private val partition = new TopicPartition("topic", 0)

  private def offset(value: Long): TopicPartitionOffset = partition -> value

  private def held(value: Long, deliveryCount: Short = 1): Held = {
    val record: KafkaByteConsumerRecord =
      new ConsumerRecord[Array[Byte], Array[Byte]]("topic", 0, value, null, null)
    Held(deliveryCount, record, Duration.Zero, Duration.Zero)
  }

  // The records the max processing time used to find by scanning the held ones.
  private def scanned(state: ShareState[IO]): Set[(FiniteDuration, TopicPartitionOffset)] =
    state.unacked.collect { case (p, h) if !state.unpulled(p) => h.heldSince -> p }.toSet

  private def record(value: Long): KafkaByteConsumerRecord =
    new ConsumerRecord[Array[Byte], Array[Byte]]("topic", 0, value, null, null)

  describe("ShareState") {
    it("should keep the timed records equal to a scan of the held ones") {
      def checked(state: ShareState[IO]): ShareState[IO] = {
        assert(state.timed.toSet == scanned(state))
        state
      }
      val ack = (p: Long, kind: AcknowledgeType) => Map(offset(p) -> kind)
      val s0 = checked(
        ShareState
          .initial[IO](implicitMode = false, paused = false)
          .polled(
            Vector.empty,
            Vector(0L, 1L, 2L, 3L).map(p => offset(p) -> held(p)),
            Vector.empty,
            None
          )
      )
      // Taken by the stream: all four are timed from 5.
      val s1 = checked(s0.withHandedOut(s0.pollId, 5.seconds))
      assert(s1.timed.size == 4 && s1.nextOverdue(10.seconds).contains(15.seconds))
      // Accepted, renewed by the user, and gone.
      val s2 = checked(
        s1.withAcked(
          ack(0, AcknowledgeType.Accept),
          Set.empty,
          Vector.empty,
          7.seconds,
          byUser = true
        )
      )
      val s3 = checked(
        s2.withAcked(
          ack(1, AcknowledgeType.Renew),
          Set(offset(2)),
          Vector.empty,
          7.seconds,
          byUser = true
        )
      )
      assert(s3.overdue(10.seconds, 15.seconds) == Vector(offset(3)))
      // Released, then a poll returns the renewed record, timed from its renewal by the user.
      val s4 = checked(
        s3.withAcked(
          ack(3, AcknowledgeType.Release),
          Set.empty,
          Vector.empty,
          8.seconds,
          byUser = false
        )
      )
      val s5 = checked(s4.polled(Vector(offset(1) -> record(1)), Vector.empty, Vector.empty, None))
      assert(s5.timed.toVector == Vector(7.seconds -> offset(1)))
      // A new poll the stream does not take is timed once it is released.
      val s6 = checked(
        s5.withAcked(
          ack(1, AcknowledgeType.Accept),
          Set.empty,
          Vector.empty,
          9.seconds,
          byUser = true
        )
      )
      val s7 = checked(
        s6.polled(Vector.empty, Vector(offset(4) -> held(4)), Vector(offset(5) -> held(5)), None)
      )
      assert(s7.timed.map(_._2) == SortedSet(offset(5)))
      checked(s7.withoutUnpulled._1)
    }

    it("should admit no new records while paused, but poll for renewed ones") {
      val renewing = ShareState[IO](
        held = Map(offset(0) -> held(0)),
        renewing = Set(offset(0)),
        started = true,
        paused = true
      )
      assert(!renewing.admitting && renewing.canPoll)
      assert(!ShareState[IO](started = true, paused = true).canPoll)
      assert(ShareState[IO](started = true).admitting)
    }

    it("should fail a waiter for a lost delivery while a new delivery of the record waits") {
      def waiter(deliveryCount: Short) = Waiter[IO](Set(offset(0) -> deliveryCount), _ => IO.unit)
      // Delivery 1 was lost; delivery 2 is renewing, with an acknowledgement waiting for it.
      val state = ShareState[IO](
        held = Map(offset(0) -> held(0, deliveryCount = 2)),
        renewing = Set(offset(0)),
        deferred = Map(offset(0) -> AcknowledgeType.Accept),
        waiters = Vector(waiter(1), waiter(2))
      )
      val (afterLost, failed) = state.settledLost
      assert(failed.size == 1 && afterLost.waiters.map(_.deliveries) == Vector(Set(offset(0) -> 2)))
      val (afterReturned, completed) = afterLost.settled(offset(0), 2, Right(()))
      assert(completed.size == 1 && afterReturned.waiters.isEmpty)
    }

    it("should release an untaken poll, its renewing records once a poll returns them") {
      val state = ShareState[IO](
        held = Map(offset(0) -> held(0), offset(1) -> held(1), offset(2) -> held(2)),
        acked = Set(offset(2)),
        renewing = Set(offset(1)),
        unpulled = Set(offset(0), offset(1), offset(2))
      )
      val (next, toRelease) = state.withoutUnpulled
      assert(toRelease == Vector(offset(0)) && next.unpulled.isEmpty)
      assert(next.deferred == Map(offset(1) -> AcknowledgeType.Release))
    }

    it("should hold no new records in implicit mode, and accept a poll once the stream took it") {
      val polled = ShareState
        .initial[IO](implicitMode = true, paused = false)
        .polled(Vector.empty, Vector(offset(0) -> held(0)), Vector.empty, None)
      assert(polled.held.isEmpty && polled.unpulled == Set(offset(0)) && !polled.acceptPending)
      assert(polled.inFlight == 1)
      assert(polled.withHandedOut(polled.pollId, Duration.Zero).acceptPending)
      assert(!polled.withHandedOut(polled.pollId - 1, Duration.Zero).acceptPending)
    }
  }

}
