/*
 * Copyright 2018 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka.internal

import org.apache.kafka.common.TopicPartition

package object actor {

  // A record's offset in its topic partition. It identifies a record, not a delivery: a redelivery
  // has the same one.
  type TopicPartitionOffset = (TopicPartition, Long)

}
