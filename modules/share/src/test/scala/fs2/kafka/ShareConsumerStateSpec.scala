/*
 * Copyright 2018 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka

import fs2.kafka.internal.Delivery
import fs2.kafka.internal.ShareConsumerState
import org.apache.kafka.clients.consumer.ConsumerRecord as JavaRecord
import org.apache.kafka.common.TopicPartition
import scala.concurrent.duration.*

final class ShareConsumerStateSpec extends BaseSpec {

  private val partition = new TopicPartition("topic", 0)

  private def delivery(offset: Long) =
    (partition -> offset) -> Delivery(
      new JavaRecord("topic", 0, offset, Array.emptyByteArray, Array.emptyByteArray),
      1,
      Duration.Zero
    )

  private val state = ShareConsumerState
    .polled(Vector(delivery(0L), delivery(1L)), Duration.Zero, None, ShareConsumerState.empty)

  private val (first, second) = (partition -> 0L, partition -> 1L)

  private val renewing =
    state.withRenewing(Set(first, second), Set.empty, Set.empty, 1.second, None)

  private def record(offset: Long) =
    new JavaRecord("topic", 0, offset, Array.emptyByteArray, Array.emptyByteArray)

  describe("ShareConsumerState") {
    it("should have nothing to send once Java lost every pending acknowledgement") {
      val acked = state.withAcked(Vector(partition -> 0L), 1.second)
      acked.nextSend(1.second) shouldBe Some(2.seconds)
      acked.withoutHeld(Vector(partition -> 0L)).nextSend(1.second) shouldBe None
    }

    it("should keep an acknowledgement made while renewing until the record is back") {
      renewing.holds(first, 1) shouldBe true
      renewing.canTakeBack shouldBe false
      val confirmed = renewing
        .withKept(Vector(first -> AcknowledgeType.Accept))
        .withResults(Vector(first -> true, second -> true), 2.seconds, Duration.Zero)
        ._1
      confirmed.canTakeBack shouldBe true
      val (back, (acks, unknown)) =
        confirmed.withTakenBack(Vector(first -> record(0L), second -> record(1L)), None)
      acks shouldBe Vector(first -> AcknowledgeType.Accept)
      unknown shouldBe empty
      back.hasRenewing shouldBe false
      back.held.keySet shouldBe Set(first, second)
    }

    it("should lose a record whose renewal failed") {
      val (next, failed) =
        renewing.withResults(Vector(first -> false), 2.seconds, Duration.Zero)
      failed shouldBe Set(first)
      next.holds(first, 1) shouldBe false
      next.holds(second, 1) shouldBe true
    }

    it("should forget a renewal Java can no longer report") {
      renewing.withResults(Vector.empty, 2.seconds, 2.seconds)._2 shouldBe Set(first, second)
    }

    it("should release a record still being renewed when process returned, once back") {
      val (_, (acks, _)) = renewing.closed
        .withResults(Vector(first -> true, second -> true), 2.seconds, Duration.Zero)
        ._1
        .withTakenBack(Vector(first -> record(0L), second -> record(1L)), None)
      acks.toMap shouldBe Map(first -> AcknowledgeType.Release, second -> AcknowledgeType.Release)
    }

    it("should keep the send deadline while an acknowledgement is pending") {
      val acked = state.withAcked(Vector(partition -> 0L, partition -> 1L), 1.second)
      acked.withoutHeld(Vector(partition -> 0L)).nextSend(1.second) shouldBe Some(2.seconds)
    }
  }

}
