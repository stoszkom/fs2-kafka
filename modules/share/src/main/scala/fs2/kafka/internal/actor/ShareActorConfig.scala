/*
 * Copyright 2018 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka.internal.actor

import scala.concurrent.duration.*

import cats.effect.Sync
import fs2.kafka.ShareConsumerSettings

import org.apache.kafka.clients.consumer.ConsumerConfig

/**
  * What a [[KafkaShareConsumerActor]] reads from its settings and the Java consumer's properties,
  * resolved once.
  *
  * @param settings
  *   the settings, with automatic renewal off in implicit acknowledgement mode, where Java does not
  *   allow renewal
  * @param implicitMode
  *   whether Java accepts a poll's records on the next poll (`share.acknowledgement.mode`)
  */
final private[kafka] case class ShareActorConfig[F[_], K, V](
  settings: ShareConsumerSettings[F, K, V],
  maxPollInterval: FiniteDuration,
  apiTimeout: FiniteDuration,
  implicitMode: Boolean
)

private[kafka] object ShareActorConfig {

  def from[F[_], K, V](
    settings: ShareConsumerSettings[F, K, V]
  )(implicit F: Sync[F]): F[ShareActorConfig[F, K, V]] =
    F.delay {
      def property[A](key: String, default: A)(parse: String => A): A =
        settings.properties.get(key).fold(default)(v => parse(v.trim))

      val implicitMode =
        property(ConsumerConfig.SHARE_ACKNOWLEDGEMENT_MODE_CONFIG, true)(
          _ == "implicit"
        )

      ShareActorConfig(
        settings = if (implicitMode) settings.withAutoRenewal(false) else settings,
        maxPollInterval = property(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 300000L)(_.toLong)
          .millis,
        apiTimeout = property(ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, 60000L)(_.toLong)
          .millis,
        implicitMode = implicitMode
      )
    }

}
