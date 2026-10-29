/*
 * Copyright 2018 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka

import scala.concurrent.duration.*

import cats.effect.Resource

import org.apache.kafka.clients.consumer.ConsumerConfig

/**
  * [[ShareConsumerSettings]] contain settings necessary to create a [[KafkaShareConsumer]]. At the
  * very least, this includes key and value deserializers.<br><br>
  *
  * Unlike Java's share consumer, which defaults to `implicit` acknowledgement and `batch_optimized`
  * acquisition, these settings default to `explicit` acknowledgement, where every polled record
  * needs an acknowledgement before the next poll, and `record_limit` acquisition, where a poll
  * returns at most `max.poll.records` records. Both modes can be changed with [[withProperty]], and
  * behave as in Java's share consumer. As in Java, records are not renewed automatically unless
  * enabled with [[withAutoRenewal]].<br><br>
  *
  * Several convenience functions are provided so that you don't have to work with `String` values
  * and `ConsumerConfig` for configuration. It's still possible to specify `ConsumerConfig` values
  * with functions like [[withProperty]].<br><br>
  *
  * [[ShareConsumerSettings]] instances are immutable and all modification functions return a new
  * [[ShareConsumerSettings]] instance.<br><br>
  *
  * Use `ShareConsumerSettings#apply` to create a new instance.<br><br>
  *
  * Experimental: this API targets Kafka's new share groups and may change in minor releases.
  */
sealed abstract class ShareConsumerSettings[F[_], K, V] {

  /**
    * The `Deserializer` to use for deserializing record keys.
    */
  def keyDeserializer: Resource[F, KeyDeserializer[F, K]]

  /**
    * The `Deserializer` to use for deserializing record values.
    */
  def valueDeserializer: Resource[F, ValueDeserializer[F, V]]

  /**
    * Properties which can be provided when creating a Java `KafkaShareConsumer` instance. Numerous
    * functions in [[ShareConsumerSettings]] add properties here if the settings are used by the
    * Java `KafkaShareConsumer`.
    */
  def properties: Map[String, String]

  /**
    * Returns a new [[ShareConsumerSettings]] instance with the specified bootstrap servers. This is
    * equivalent to setting the following property using the [[withProperty]] function.
    *
    * {{{
    * ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG
    * }}}
    */
  final def withBootstrapServers(servers: String): ShareConsumerSettings[F, K, V] =
    withProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, servers)

  /**
    * Returns a new [[ShareConsumerSettings]] instance with the specified client id. This is
    * equivalent to setting the following property using the [[withProperty]] function.
    *
    * {{{
    * ConsumerConfig.CLIENT_ID_CONFIG
    * }}}
    */
  final def withClientId(clientId: String): ShareConsumerSettings[F, K, V] =
    withProperty(ConsumerConfig.CLIENT_ID_CONFIG, clientId)

  /**
    * Returns a new [[ShareConsumerSettings]] instance with the specified share group id. This is
    * equivalent to setting the following property using the [[withProperty]] function.
    *
    * {{{
    * ConsumerConfig.GROUP_ID_CONFIG
    * }}}
    */
  final def withGroupId(groupId: String): ShareConsumerSettings[F, K, V] =
    withProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId)

  /**
    * Returns a new [[ShareConsumerSettings]] instance with the specified max poll records. This is
    * equivalent to setting the following property using the [[withProperty]] function, except you
    * can specify it with an `Int` instead of a `String`. With `record_limit` acquisition, the
    * default, it is also the most records a consumer holds at once.
    *
    * {{{
    * ConsumerConfig.MAX_POLL_RECORDS_CONFIG
    * }}}
    */
  final def withMaxPollRecords(maxPollRecords: Int): ShareConsumerSettings[F, K, V] =
    withProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, maxPollRecords.toString)

  /**
    * Returns a new [[ShareConsumerSettings]] instance with the specified max poll interval. This is
    * equivalent to setting the following property using the [[withProperty]] function, except you
    * can specify it with a `FiniteDuration` instead of a `String`.
    *
    * {{{
    * ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG
    * }}}
    */
  final def withMaxPollInterval(interval: FiniteDuration): ShareConsumerSettings[F, K, V] =
    withProperty(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, interval.toMillis.toString)

  /**
    * Includes a property with the specified `key` and `value`. The key should be one of the keys in
    * `ConsumerConfig`, and the value should be a valid choice for the key.
    */
  def withProperty(key: String, value: String): ShareConsumerSettings[F, K, V]

  /**
    * Includes the specified keys and values as properties. The keys should be part of the
    * `ConsumerConfig` keys, and the values should be valid choices for the keys.
    */
  final def withProperties(properties: (String, String)*): ShareConsumerSettings[F, K, V] =
    withProperties(properties.toMap)

  /**
    * Includes the specified keys and values as properties. The keys should be part of the
    * `ConsumerConfig` keys, and the values should be valid choices for the keys.
    */
  def withProperties(properties: Map[String, String]): ShareConsumerSettings[F, K, V]

  /**
    * The time to wait for the Java `KafkaShareConsumer` to shutdown.<br><br>
    *
    * The default value is 20 seconds.
    */
  def closeTimeout: FiniteDuration

  /**
    * Creates a new [[ShareConsumerSettings]] with the specified [[closeTimeout]].
    */
  def withCloseTimeout(timeout: FiniteDuration): ShareConsumerSettings[F, K, V]

  /**
    * The time to wait for the broker to confirm committed acknowledgements. If they are not
    * confirmed within this time, a [[ShareCommitTimeoutException]] will be raised instead.<br><br>
    *
    * The default value is 15 seconds.
    */
  def commitTimeout: FiniteDuration

  /**
    * Creates a new [[ShareConsumerSettings]] with the specified [[commitTimeout]]. The value is
    * used exactly as given.<br><br>
    *
    * With [[withAutoRenewal]] enabled, the timeout must leave time to renew locks behind a commit:
    * it must stay below the acquisition lock duration minus a safety margin of
    * `min(lock / 10, 1 second)`, or the stream fails with a `ConfigException` once the broker
    * reports the lock duration.
    */
  def withCommitTimeout(timeout: FiniteDuration): ShareConsumerSettings[F, K, V]

  /**
    * How often we should attempt to call `poll` on the Java `KafkaShareConsumer`. It also limits
    * the waits between checks for commit confirmations, and is the renewal interval until the
    * broker reports its acquisition lock duration.<br><br>
    *
    * The default value is 50 milliseconds.
    */
  def pollInterval: FiniteDuration

  /**
    * Creates a new [[ShareConsumerSettings]] with the specified [[pollInterval]].
    */
  def withPollInterval(interval: FiniteDuration): ShareConsumerSettings[F, K, V]

  /**
    * How long we should allow the `poll` call to block for in the Java
    * `KafkaShareConsumer`.<br><br>
    *
    * The default value is 50 milliseconds.
    */
  def pollTimeout: FiniteDuration

  /**
    * Creates a new [[ShareConsumerSettings]] with the specified [[pollTimeout]].
    */
  def withPollTimeout(timeout: FiniteDuration): ShareConsumerSettings[F, K, V]

  /**
    * Whether records without an acknowledgement are renewed automatically before their acquisition
    * lock expires, at about half of the lock duration. Renewal does not apply in implicit
    * acknowledgement mode, where Java does not allow it.<br><br>
    *
    * The default value is `false`, as in Java's share consumer.
    */
  def autoRenewal: Boolean

  /**
    * Creates a new [[ShareConsumerSettings]] with the specified [[autoRenewal]].<br><br>
    *
    * Without automatic renewal, a record which is neither acknowledged nor renewed in time is
    * delivered again, possibly to another consumer, and its later acknowledgement fails. Enable it
    * when processing may outlast the lock. A record whose processing hangs is then renewed, and
    * holds back the next poll of its consumer, until the stream ends.
    */
  def withAutoRenewal(enabled: Boolean): ShareConsumerSettings[F, K, V]

}

object ShareConsumerSettings {

  final private case class Impl[F[_], K, V](
    keyDeserializer: Resource[F, KeyDeserializer[F, K]],
    valueDeserializer: Resource[F, ValueDeserializer[F, V]],
    properties: Map[String, String],
    closeTimeout: FiniteDuration,
    pollTimeout: FiniteDuration,
    pollInterval: FiniteDuration,
    commitTimeout: FiniteDuration,
    autoRenewal: Boolean
  ) extends ShareConsumerSettings[F, K, V] {

    override def withProperty(key: String, value: String): ShareConsumerSettings[F, K, V] =
      copy(properties = properties.updated(key, value))

    override def withProperties(values: Map[String, String]): ShareConsumerSettings[F, K, V] =
      copy(properties = properties ++ values)

    override def withCloseTimeout(timeout: FiniteDuration): ShareConsumerSettings[F, K, V] = {
      require(timeout >= Duration.Zero, "close timeout must be non-negative")
      copy(closeTimeout = timeout)
    }

    override def withPollTimeout(timeout: FiniteDuration): ShareConsumerSettings[F, K, V] = {
      require(timeout > Duration.Zero, "poll timeout must be positive")
      copy(pollTimeout = timeout)
    }

    override def withPollInterval(interval: FiniteDuration): ShareConsumerSettings[F, K, V] = {
      require(interval > Duration.Zero, "poll interval must be positive")
      copy(pollInterval = interval)
    }

    override def withCommitTimeout(timeout: FiniteDuration): ShareConsumerSettings[F, K, V] = {
      require(timeout > Duration.Zero, "commit timeout must be positive")
      copy(commitTimeout = timeout)
    }

    override def withAutoRenewal(enabled: Boolean): ShareConsumerSettings[F, K, V] =
      copy(autoRenewal = enabled)

    override def toString: String =
      s"ShareConsumerSettings(closeTimeout = $closeTimeout, pollTimeout = $pollTimeout, pollInterval = $pollInterval, commitTimeout = $commitTimeout, autoRenewal = $autoRenewal)"

  }

  def apply[F[_], K, V](
    keyDeserializer: KeyDeserializer[F, K],
    valueDeserializer: ValueDeserializer[F, V]
  ): ShareConsumerSettings[F, K, V] =
    apply(
      Resource.pure[F, KeyDeserializer[F, K]](keyDeserializer),
      Resource.pure[F, ValueDeserializer[F, V]](valueDeserializer)
    )

  def apply[F[_], K, V](implicit
    keyDeserializer: Resource[F, KeyDeserializer[F, K]],
    valueDeserializer: Resource[F, ValueDeserializer[F, V]]
  ): ShareConsumerSettings[F, K, V] =
    Impl(
      keyDeserializer,
      valueDeserializer,
      Map(
        "share.acknowledgement.mode" -> "explicit",
        "share.acquire.mode"         -> "record_limit"
      ),
      20.seconds,
      50.millis,
      50.millis,
      15.seconds,
      autoRenewal = false
    )

}
