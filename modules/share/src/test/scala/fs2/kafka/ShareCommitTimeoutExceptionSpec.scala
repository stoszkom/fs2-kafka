/*
 * Copyright 2018 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka

import scala.concurrent.duration.*

final class ShareCommitTimeoutExceptionSpec extends BaseSpec {
  describe("ShareCommitTimeoutException") {
    it("should have expected message and toString") {
      val exception = ShareCommitTimeoutException(10.seconds)
      assert {
        exception.getMessage == "share commit timeout after 10 seconds" &&
        exception.toString ==
          "fs2.kafka.ShareCommitTimeoutException: share commit timeout after 10 seconds"
      }
    }

    it("should have no cause unless given one, so that one can still be set") {
      val exception = ShareCommitTimeoutException(10.seconds)
      assert(exception.getCause == null)
      val cause = new RuntimeException("cause")
      exception.initCause(cause)
      assert(exception.getCause == cause)
    }

    it("should keep the given cause") {
      val cause = new org.apache.kafka.common.errors.TimeoutException("request timed out")
      assert(ShareCommitTimeoutException(10.seconds, cause).getCause == cause)
    }
  }
}
