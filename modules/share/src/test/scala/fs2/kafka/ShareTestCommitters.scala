/*
 * Copyright 2018 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka

import java.util.concurrent.ConcurrentLinkedQueue

import scala.concurrent.duration.*

import cats.effect.IO

import org.apache.kafka.common.{KafkaException, TopicPartition}

object ShareTestCommitters {

  // A committer which records the offsets it commits, after `delay`, and reports `results` for
  // their partitions.
  def committer(
    committed: ConcurrentLinkedQueue[List[Long]],
    delay: FiniteDuration = Duration.Zero,
    pending: Int = Int.MaxValue,
    results: Map[TopicPartition, Option[KafkaException]] = Map.empty
  ): KafkaShareCommitter[IO] =
    KafkaShareCommitter[IO](
      _ => IO.unit,
      acks => IO.sleep(delay) *> IO(committed.add(acks.toList.map(_.offset))).void,
      acks =>
        IO.sleep(delay) *> IO(committed.add(acks.toList.map(_.offset))).as(
          acks.toList.map(a => a.topicPartition -> results.get(a.topicPartition).flatten).toMap
        ),
      IO.pure(pending)
    )

  def acknowledged(
    committer: KafkaShareCommitter[IO],
    offset: Long,
    partition: Int = 0
  ): AcknowledgedOffset[IO] =
    AcknowledgeableOffset(new TopicPartition("topic", partition), offset, 1.toShort, committer)
      .acknowledge(AcknowledgeType.Accept)

}
