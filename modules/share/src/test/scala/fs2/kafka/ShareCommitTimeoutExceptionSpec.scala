/*
 * Copyright 2018 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka

import scala.concurrent.duration.*

import org.apache.kafka.common.TopicPartition

final class ShareCommitTimeoutExceptionSpec extends BaseSpec {
  describe("ShareCommitTimeoutException") {
    it("should have expected message and toString") {
      val exception =
        ShareCommitTimeoutException(
          10.seconds,
          Map(
            new TopicPartition("topic", 1) -> Map(3L -> AcknowledgeType.Accept),
            new TopicPartition("topic", 0) -> Map(
              2L -> AcknowledgeType.Renew,
              0L -> AcknowledgeType.Accept
            )
          )
        )

      assert {
        exception
          .getMessage == "share commit timeout after 10 seconds for offsets: topic-0 -> {0: Accept, 2: Renew}, topic-1 -> {3: Accept}" &&
        exception
          .toString == "fs2.kafka.ShareCommitTimeoutException: share commit timeout after 10 seconds for offsets: topic-0 -> {0: Accept, 2: Renew}, topic-1 -> {3: Accept}"
      }
    }
  }
}
