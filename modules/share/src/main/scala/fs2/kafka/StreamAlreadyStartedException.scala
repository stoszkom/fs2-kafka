/*
 * Copyright 2018 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka

import org.apache.kafka.common.KafkaException

/**
  * [[StreamAlreadyStartedException]] indicates that the record stream of a share consumer was
  * started more than once. Each share consumer has a single record stream.
  */
sealed abstract class StreamAlreadyStartedException
  extends KafkaException("the record stream of this share consumer has already been started")

private[kafka] object StreamAlreadyStartedException {

  def apply(): StreamAlreadyStartedException =
    new StreamAlreadyStartedException {

      override def toString: String =
        s"fs2.kafka.StreamAlreadyStartedException: $getMessage"

    }

}
