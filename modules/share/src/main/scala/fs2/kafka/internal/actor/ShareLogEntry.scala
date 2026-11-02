/*
 * Copyright 2018 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka.internal.actor

import scala.concurrent.duration.FiniteDuration

import fs2.kafka.internal.{LogEntry, LogLevel}
import fs2.kafka.internal.LogLevel.{Debug, Warn}

/**
  * What a [[KafkaShareConsumerActor]] logs, like `LogEntry` for `KafkaConsumerActor`.
  */
sealed abstract private[kafka] class ShareLogEntry extends LogEntry

private[kafka] object ShareLogEntry {

  private def describe(records: Iterable[TopicPartitionOffset]): String =
    records.map { case (partition, offset) => s"$partition@$offset" }.mkString(", ")

  final case class Polled(records: Int, renewed: Int, inFlight: Int) extends ShareLogEntry {

    override def level: LogLevel = Debug

    override def message: String =
      s"Polled $records records, of which $renewed renewed ones returned. Records in flight: $inFlight."

  }

  final case class ReleasingOverdue(
    records: Vector[TopicPartitionOffset],
    maxProcessingTime: FiniteDuration
  ) extends ShareLogEntry {

    override def level: LogLevel = Warn

    override def message: String = {
      val shown = describe(records.take(10)) + (if (records.size > 10) ", ..." else "")
      s"Releasing ${records.size} records held without an acknowledgement past the max processing " +
        s"time ($maxProcessingTime), so they are delivered again: $shown."
    }

  }

}
