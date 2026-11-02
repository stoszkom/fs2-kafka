/*
 * Copyright 2018 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka.internal.actor

import scala.concurrent.duration.*

import fs2.kafka.internal.converters.option.*
import fs2.kafka.KafkaByteShareConsumer

/**
  * The acquisition lock reported by the broker.
  */
private[kafka] object ShareTimeouts {

  /**
    * The acquisition lock duration last reported by the broker.
    */
  def acquisitionLock(consumer: KafkaByteShareConsumer): Option[FiniteDuration] =
    consumer.acquisitionLockTimeoutMs().toScala.map(_.intValue.millis)

}
