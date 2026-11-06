/*
 * Copyright 2018 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka.internal

import cats.effect.Sync
import fs2.kafka.ShareConsumerSettings
import org.apache.kafka.clients.consumer.ConsumerConfig
import scala.concurrent.duration.*

/**
  * What a share consumer reads from its settings and the Java consumer's properties, resolved once.
  *
  * @param implicitMode
  *   whether Java accepts a poll's records on the next poll (`share.acknowledgement.mode`)
  */
final private[kafka] case class ShareConsumerConfig[F[_], K, V](
  settings: ShareConsumerSettings[F, K, V],
  maxPollInterval: FiniteDuration,
  apiTimeout: FiniteDuration,
  implicitMode: Boolean
) {

  // How long a renewal waits for the broker: past the margin, or half the lock, the lock has likely
  // ended.
  def renewalWait(lock: Option[FiniteDuration]): FiniteDuration =
    lock.fold(settings.renewalMargin)(_ / 2).min(settings.renewalMargin)

  // The renewal margin before the lock ends, or half the lock, early enough for
  // `max.poll.interval.ms`.
  def renewalInterval(lock: Option[FiniteDuration]): FiniteDuration =
    lock.fold(ShareConsumerConfig.unknownLockRenewalInterval) { lock =>
      val byMargin =
        if (settings.renewalMargin < lock) lock - settings.renewalMargin else lock / 2
      byMargin.min(maxPollInterval / 2)
    }

}

private[kafka] object ShareConsumerConfig {

  // Half of the shortest lock a broker allows, while the broker has not reported the lock.
  val unknownLockRenewalInterval: FiniteDuration = 500.millis

  def from[F[_], K, V](
    settings: ShareConsumerSettings[F, K, V]
  )(implicit F: Sync[F]): F[ShareConsumerConfig[F, K, V]] =
    F.delay {
      def property[A](key: String, default: A)(parse: String => A): A =
        settings.properties.get(key).fold(default)(v => parse(v.trim))

      val implicitMode =
        property(ConsumerConfig.SHARE_ACKNOWLEDGEMENT_MODE_CONFIG, true)(
          _ == "implicit"
        )

      ShareConsumerConfig(
        settings = settings,
        maxPollInterval = property(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 300000L)(_.toLong).millis,
        apiTimeout = property(ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, 60000L)(_.toLong).millis,
        implicitMode = implicitMode
      )
    }

}
