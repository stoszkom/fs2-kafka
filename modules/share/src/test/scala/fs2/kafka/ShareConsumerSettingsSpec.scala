/*
 * Copyright 2018 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka

import scala.concurrent.duration.*

import cats.effect.IO
import fs2.kafka.internal.actor.ShareTimeouts

import org.apache.kafka.common.config.ConfigException

final class ShareConsumerSettingsSpec extends BaseSpec {

  private val settings = ShareConsumerSettings[IO, String, String]

  private def commitTimeout(
    s: ShareConsumerSettings[IO, String, String],
    lock: Option[FiniteDuration]
  ) =
    ShareTimeouts.effectiveCommitTimeout(s, lock)

  describe("ShareConsumerSettings") {
    it("should default to explicit acknowledgement, record_limit acquisition and no renewal") {
      settings.properties shouldBe Map(
        "share.acknowledgement.mode" -> "explicit",
        "share.acquire.mode"         -> "record_limit"
      )
      settings.autoRenewal shouldBe false
      settings.withAutoRenewal(true).autoRenewal shouldBe true
    }

    it("should reject invalid timeouts") {
      intercept[IllegalArgumentException](settings.withCloseTimeout((-1).second))
      intercept[IllegalArgumentException](settings.withPollTimeout(Duration.Zero))
      intercept[IllegalArgumentException](settings.withPollInterval(Duration.Zero))
      intercept[IllegalArgumentException](settings.withCommitTimeout(Duration.Zero))
    }

    it("should use a 15 second commit timeout while the lock is unknown") {
      commitTimeout(settings, None) shouldBe Right(15.seconds)
    }

    it("should use the commit timeout as it is without automatic renewal") {
      commitTimeout(settings, Some(600.millis)) shouldBe Right(15.seconds)
      commitTimeout(settings.withCommitTimeout(1.second), Some(600.millis)) shouldBe Right(
        1.second
      )
    }

    it("should use the default commit timeout with automatic renewal when it leaves time to renew") {
      commitTimeout(settings.withAutoRenewal(true), Some(30.seconds)) shouldBe Right(15.seconds)
    }

    it("should use an explicit commit timeout which leaves time to renew") {
      commitTimeout(
        settings.withAutoRenewal(true).withCommitTimeout(5.seconds),
        Some(30.seconds)
      ) shouldBe Right(5.seconds)
    }

    it("should reject a commit timeout which leaves no time to renew") {
      val renewing = settings.withAutoRenewal(true)
      commitTimeout(renewing.withCommitTimeout(1.second), Some(600.millis))
        .swap
        .toOption
        .get shouldBe
        a[ConfigException]
      commitTimeout(renewing, Some(15.seconds)).swap.toOption.get shouldBe a[ConfigException]
    }
  }

}
