/*
 * Copyright 2018 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka.internal.actor

import scala.concurrent.duration.*

import cats.effect.IO
import fs2.kafka.{BaseSpec, KafkaByteConsumerRecord}

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition

final class ShareStateSpec extends BaseSpec {

  private val partition = new TopicPartition("topic", 0)

  private def offset(value: Long): TopicPartitionOffset = partition -> value

  private def held(value: Long, deliveryCount: Short = 1): Held = {
    val record: KafkaByteConsumerRecord =
      new ConsumerRecord[Array[Byte], Array[Byte]]("topic", 0, value, null, null)
    Held(deliveryCount, record, Duration.Zero, Duration.Zero)
  }

  describe("ShareState") {
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
  }

}
