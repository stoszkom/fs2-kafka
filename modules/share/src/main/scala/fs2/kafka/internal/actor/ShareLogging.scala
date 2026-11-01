/*
 * Copyright 2018 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka.internal.actor

import scala.concurrent.duration.FiniteDuration

import cats.effect.Sync
import cats.syntax.all.*
import fs2.kafka.internal.LogLevel
import fs2.kafka.internal.LogLevel.{Debug, Warn}

import org.slf4j.LoggerFactory

/**
  * What a [[KafkaShareConsumerActor]] logs, like `LogEntry` for `KafkaConsumerActor`.
  */
sealed abstract private[kafka] class ShareLogEntry {

  def level: LogLevel

  def message: String

}

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
      s"Releasing ${records.size} records held without a decision past the max processing " +
        s"time ($maxProcessingTime), so they are delivered again: $shown."
    }

  }

}

/**
  * Logs [[ShareLogEntry]]s with SLF4J.
  */
sealed abstract private[kafka] class ShareLogging[F[_]] {

  def log(entry: ShareLogEntry): F[Unit]

}

private[kafka] object ShareLogging {

  def create[F[_]](name: String)(implicit F: Sync[F]): F[ShareLogging[F]] =
    F.delay(LoggerFactory.getLogger(name))
      .map { logger =>
        new ShareLogging[F] {

          override def log(entry: ShareLogEntry): F[Unit] =
            F.delay {
              entry.level match {
                case LogLevel.Error =>
                  if (logger.isErrorEnabled) logger.error(entry.message)
                case LogLevel.Warn =>
                  if (logger.isWarnEnabled) logger.warn(entry.message)
                case LogLevel.Info =>
                  if (logger.isInfoEnabled) logger.info(entry.message)
                case LogLevel.Debug =>
                  if (logger.isDebugEnabled) logger.debug(entry.message)
              }
            }

        }
      }

}
