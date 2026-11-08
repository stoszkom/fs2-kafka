/*
 * Copyright 2018 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka.internal

import fs2.kafka.internal.Acknowledgement.TopicPartitionOffset
import fs2.kafka.internal.converters.collection.*
import org.apache.kafka.clients.consumer.AcknowledgementCommitCallback
import org.apache.kafka.common.TopicIdPartition

// Collects what Java reports of each sent acknowledgement, renewals included, from its commit
// callback. Java runs the callback during a poll or commit, on the consumer's single thread, which
// is also the only thread draining it.
final private[kafka] class AcknowledgementResults extends AcknowledgementCommitCallback {

  private val results = Vector.newBuilder[(TopicPartitionOffset, Boolean)]

  override def onComplete(
    offsets: java.util.Map[TopicIdPartition, java.util.Set[java.lang.Long]],
    exception: Exception
  ): Unit =
    offsets.asScala.foreach { case (partition, sent) =>
      sent.asScala.foreach { offset =>
        results += ((partition.topicPartition -> offset.longValue) -> (exception == null))
      }
    }

  // What was reported since the last drain: each offset with whether the broker took it.
  def drain(): Vector[(TopicPartitionOffset, Boolean)] = {
    val drained = results.result()
    results.clear()
    drained
  }

}
