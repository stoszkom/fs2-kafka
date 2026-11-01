/*
 * Copyright 2018 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka.internal.actor

import scala.concurrent.duration.*

import fs2.kafka.{KafkaByteShareConsumer, ShareConsumerSettings}

import org.apache.kafka.common.config.ConfigException

/**
  * The acquisition lock and the timeouts bound by it.
  */
private[kafka] object ShareTimeouts {

  /**
    * The acquisition lock duration last reported by the broker.
    */
  def acquisitionLock(consumer: KafkaByteShareConsumer): Option[FiniteDuration] = {
    val lock = consumer.acquisitionLockTimeoutMs()
    if (lock.isPresent) Some(lock.get.intValue.millis) else None
  }

  private[kafka] def safetyMargin(lock: FiniteDuration): FiniteDuration =
    (lock / 10).min(1.second)

  private[kafka] def latestRenewal(
    lock: FiniteDuration,
    commitTimeout: FiniteDuration
  ): FiniteDuration =
    lock - commitTimeout - safetyMargin(lock)

  /**
    * The commit timeout, rejected with automatic renewal when the lock leaves no time to renew
    * behind a commit.
    */
  private[kafka] def effectiveCommitTimeout[F[_], K, V](
    settings: ShareConsumerSettings[F, K, V],
    lock: Option[FiniteDuration]
  ): Either[ConfigException, FiniteDuration] =
    lock match {
      case Some(l)
          if settings.autoRenewal && latestRenewal(l, settings.commitTimeout) <= Duration.Zero =>
        Left(
          new ConfigException(
            s"commitTimeout (${settings.commitTimeout}) leaves no time to renew the broker's " +
              s"acquisition lock ($l): a renewal can wait behind one commit, so commitTimeout must " +
              s"stay below the lock duration minus a safety margin of ${safetyMargin(l)}. Reduce " +
              "withCommitTimeout or increase the group's share.record.lock.duration.ms."
          )
        )
      case _ => Right(settings.commitTimeout)
    }

}
