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
    .polled(
      Vector(delivery(0L), delivery(1L)),
      Duration.Zero,
      None,
      1.minute,
      ShareConsumerState.empty
    )
    ._1

  describe("ShareConsumerState") {
    it("should have nothing to send once Java lost every pending acknowledgement") {
      val acked = state.withAcked(Vector(partition -> 0L), 1.second)
      acked.nextSend(1.second) shouldBe Some(2.seconds)
      acked.withoutHeld(Vector(partition -> 0L)).nextSend(1.second) shouldBe None
    }

    it("should split a poll into the records of lost renewals and the fresh ones") {
      val (kept, lost) = (delivery(0L), delivery(1L))
      val renewed =
        state.withRenewed(Map(kept._1 -> kept._2.record), Set.empty, 1.second, None)
      val (next, (returned, fresh)) =
        ShareConsumerState.polled(Vector(kept, lost), 2.seconds, None, 1.minute, renewed)
      returned.map(_.offset) shouldBe Vector(1L)
      fresh.map(_.offset) shouldBe Vector(0L)
      next.holds(kept._1, 1) shouldBe true
      next.holds(lost._1, 1) shouldBe false
    }

    it("should keep the send deadline while an acknowledgement is pending") {
      val acked = state.withAcked(Vector(partition -> 0L, partition -> 1L), 1.second)
      acked.withoutHeld(Vector(partition -> 0L)).nextSend(1.second) shouldBe Some(2.seconds)
    }
  }

}
