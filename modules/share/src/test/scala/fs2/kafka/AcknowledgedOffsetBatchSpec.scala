/*
 * Copyright 2018 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka

import java.util.concurrent.ConcurrentLinkedQueue

import cats.effect.unsafe.implicits.global
import fs2.kafka.internal.converters.collection.*
import fs2.kafka.ShareTestCommitters.*

import org.apache.kafka.common.{KafkaException, TopicPartition}

final class AcknowledgedOffsetBatchSpec extends BaseSpec {

  describe("AcknowledgedOffsetBatch") {
    it("should commit the offsets of each consumer with one call") {
      val committed       = new ConcurrentLinkedQueue[List[Long]]
      val (first, second) = (committer(committed), committer(committed))
      val batch           = AcknowledgedOffsetBatch.fromFoldable(
        List(acknowledged(first, 1L), acknowledged(second, 2L), acknowledged(first, 3L))
      )
      batch.offsets.keySet shouldBe Set(first, second)
      batch.commit.unsafeRunSync()
      committed.asScala.toSet shouldBe Set(List(1L, 3L), List(2L))
    }

    it("should build a batch from the offsets of elements") {
      val committed = new ConcurrentLinkedQueue[List[Long]]
      val c         = committer(committed)
      val batch     =
        AcknowledgedOffsetBatch.fromFoldableMap(List("a" -> 1L, "b" -> 2L)) { case (_, offset) =>
          acknowledged(c, offset)
        }
      batch.commit.unsafeRunSync()
      committed.asScala.toList shouldBe List(List(1L, 2L))
    }

    it("should build a batch from optional offsets, leaving out None") {
      val committed = new ConcurrentLinkedQueue[List[Long]]
      val c         = committer(committed)
      val batch     = AcknowledgedOffsetBatch.fromFoldableOption(
        List(Some(acknowledged(c, 1L)), None, Some(acknowledged(c, 3L)))
      )
      batch.commit.unsafeRunSync()
      committed.asScala.toList shouldBe List(List(1L, 3L))
    }

    it("should report the result of each partition across consumers") {
      val committed             = new ConcurrentLinkedQueue[List[Long]]
      val failure               = new KafkaException("failed")
      val (failing, succeeding) =
        (
          committer(committed, results = Map(new TopicPartition("topic", 0) -> Some(failure))),
          committer(committed)
        )
      val batch = AcknowledgedOffsetBatch.fromFoldable(
        List(
          acknowledged(failing, 1L),
          acknowledged(succeeding, 2L),
          acknowledged(succeeding, 3L, partition = 1)
        )
      )
      batch.commitWithResults.unsafeRunSync() shouldBe Map(
        new TopicPartition("topic", 0) -> Some(failure),
        new TopicPartition("topic", 1) -> None
      )
    }
  }

}
