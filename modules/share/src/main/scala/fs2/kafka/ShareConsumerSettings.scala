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
  * returns at most `max.poll.records` records. Both modes can be changed with
  * [[withAcknowledgementMode]] and [[withAcquireMode]], and behave as in Java's share consumer. As
  * in Java, records are not renewed automatically unless enabled with [[withAutoRenewal]], except
  * to let a renewed record be acknowledged.<br><br>
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
    * Returns a new [[ShareConsumerSettings]] instance with the specified acknowledgement mode. This
    * is equivalent to setting the following property using the [[withProperty]] function.
    *
    * {{{
    * ConsumerConfig.SHARE_ACKNOWLEDGEMENT_MODE_CONFIG
    * }}}
    */
  final def withAcknowledgementMode(
    mode: ShareAcknowledgementMode
  ): ShareConsumerSettings[F, K, V] =
    withProperty(
      ConsumerConfig.SHARE_ACKNOWLEDGEMENT_MODE_CONFIG,
      mode match {
        case ShareAcknowledgementMode.ExplicitAcknowledgement => "explicit"
        case ShareAcknowledgementMode.ImplicitAcknowledgement => "implicit"
      }
    )

  /**
    * Returns a new [[ShareConsumerSettings]] instance with the specified acquire mode. This is
    * equivalent to setting the following property using the [[withProperty]] function.
    *
    * {{{
    * ConsumerConfig.SHARE_ACQUIRE_MODE_CONFIG
    * }}}
    */
  final def withAcquireMode(mode: ShareAcquireMode): ShareConsumerSettings[F, K, V] =
    withProperty(
      ConsumerConfig.SHARE_ACQUIRE_MODE_CONFIG,
      mode match {
        case ShareAcquireMode.RecordLimitAcquire    => "record_limit"
        case ShareAcquireMode.BatchOptimizedAcquire => "batch_optimized"
      }
    )

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
    * How long we should allow the `poll` call to block for in the Java `KafkaShareConsumer`. The
    * consumer polls again as soon as the previous poll is settled and the stream has taken it, so
    * this is also how long an idle consumer waits between polls.<br><br>
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
    * delivered again, possibly to another consumer, and its later acknowledgement fails. Only while
    * an acknowledgement waits for a record you renewed are the other records renewed, so that the
    * consumer can poll the renewed record again. Enable it when processing may outlast the lock. A
    * record whose processing hangs is then renewed, and holds back the next poll of its consumer,
    * until the stream ends or the [[maxProcessingTime]] passes.
    */
  def withAutoRenewal(enabled: Boolean): ShareConsumerSettings[F, K, V]

  /**
    * How long the consumer may hold a record without a decision, or `None` for no limit. A record
    * held longer is released, so it is delivered again, possibly to another consumer, and a later
    * decision for it fails with a [[RecordNotInFlightException]]. Its processing is not cancelled.
    * It applies with explicit acknowledgement, and not to records whose decision was already
    * requested.<br><br>
    *
    * The time counts from when the stream took the record's poll, or from its latest `Renew` by the
    * user, since the consumer cannot tell when processing starts: a poll is taken as one chunk, and
    * its records may wait in the stream. Records of a poll the stream has not taken are not
    * released. Set it well above the time to process a whole poll, that is `max.poll.records` times
    * the time per record, divided by the parallelism, and above that with several consumers merged
    * into one pipeline or with `batch_optimized` polls.<br><br>
    *
    * Without a limit, a record whose processing hangs is renewed with [[withAutoRenewal]] until the
    * stream ends, and without automatic renewal it holds back its consumer, as in Java.<br><br>
    *
    * The release does not fail the stream, unless the consumer fails to send it, as for any release
    * or renewal the consumer sends itself. The later `acknowledge` fails with a
    * [[RecordNotInFlightException]], which [[acknowledgeChunks]] skips. The record is processed
    * again after its redelivery, so ignoring the failure is usually enough:
    *
    * {{{
    * records.evalMap { committable =>
    *   process(committable.record) *>
    *     committable
    *       .offset
    *       .acknowledge(AcknowledgeType.Accept)
    *       .recover { case _: RecordNotInFlightException => () }
    * }
    * }}}
    *
    * The default value is `None`.
    */
  def maxProcessingTime: Option[FiniteDuration]

  /**
    * Creates a new [[ShareConsumerSettings]] with the specified [[maxProcessingTime]].
    */
  def withMaxProcessingTime(time: FiniteDuration): ShareConsumerSettings[F, K, V]

  /**
    * Creates a new [[ShareConsumerSettings]] without a [[maxProcessingTime]], the default.
    */
  def withoutMaxProcessingTime: ShareConsumerSettings[F, K, V]

}

object ShareConsumerSettings {

  final private case class Impl[F[_], K, V](
    keyDeserializer: Resource[F, KeyDeserializer[F, K]],
    valueDeserializer: Resource[F, ValueDeserializer[F, V]],
    properties: Map[String, String],
    closeTimeout: FiniteDuration,
    pollTimeout: FiniteDuration,
    commitTimeout: FiniteDuration,
    autoRenewal: Boolean,
    maxProcessingTime: Option[FiniteDuration]
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

    override def withCommitTimeout(timeout: FiniteDuration): ShareConsumerSettings[F, K, V] = {
      require(timeout > Duration.Zero, "commit timeout must be positive")
      copy(commitTimeout = timeout)
    }

    override def withAutoRenewal(enabled: Boolean): ShareConsumerSettings[F, K, V] =
      copy(autoRenewal = enabled)

    override def withMaxProcessingTime(time: FiniteDuration): ShareConsumerSettings[F, K, V] = {
      require(time > Duration.Zero, "max processing time must be positive")
      copy(maxProcessingTime = Some(time))
    }

    override def withoutMaxProcessingTime: ShareConsumerSettings[F, K, V] =
      copy(maxProcessingTime = None)

    override def toString: String =
      s"ShareConsumerSettings(closeTimeout = $closeTimeout, pollTimeout = $pollTimeout, commitTimeout = $commitTimeout, autoRenewal = $autoRenewal, maxProcessingTime = $maxProcessingTime)"

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
      15.seconds,
      autoRenewal = false,
      maxProcessingTime = None
    )

}
