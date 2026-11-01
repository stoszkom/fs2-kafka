/*
 * Copyright 2018 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka

import scala.concurrent.duration.*

import cats.effect.IO

final class ShareConsumerSettingsSpec extends BaseSpec {

  private val settings = ShareConsumerSettings[IO, String, String]

  describe("ShareConsumerSettings") {
    it("should default to explicit acknowledgement, record_limit acquisition and no renewal") {
      settings.properties shouldBe Map(
        "share.acknowledgement.mode" -> "explicit",
        "share.acquire.mode"         -> "record_limit"
      )
      settings.autoRenewal shouldBe false
      settings.withAutoRenewal(true).autoRenewal shouldBe true
      settings.renewalMargin shouldBe 5.seconds
      settings.withRenewalMargin(10.seconds).renewalMargin shouldBe 10.seconds
      settings.maxProcessingTime shouldBe None
      settings.withMaxProcessingTime(1.minute).maxProcessingTime shouldBe Some(1.minute)
      settings
        .withMaxProcessingTime(1.minute)
        .withoutMaxProcessingTime
        .maxProcessingTime shouldBe None
      settings.maxAcknowledgementDelay shouldBe Some(1.second)
      settings.withMaxAcknowledgementDelay(Duration.Zero).maxAcknowledgementDelay shouldBe
        Some(Duration.Zero)
      settings.withoutMaxAcknowledgementDelay.maxAcknowledgementDelay shouldBe None
    }

    it("should set the acknowledgement and acquire modes") {
      settings
        .withAcknowledgementMode(ShareAcknowledgementMode.Implicit)
        .withAcquireMode(ShareAcquireMode.BatchOptimized)
        .properties shouldBe Map(
        "share.acknowledgement.mode" -> "implicit",
        "share.acquire.mode"         -> "batch_optimized"
      )
      settings
        .withAcknowledgementMode(ShareAcknowledgementMode.Explicit)
        .withAcquireMode(ShareAcquireMode.RecordLimit)
        .properties shouldBe settings.properties
    }

    it("should reject invalid timeouts") {
      intercept[IllegalArgumentException](settings.withCloseTimeout((-1).second))
      intercept[IllegalArgumentException](settings.withPollTimeout(Duration.Zero))
      intercept[IllegalArgumentException](settings.withCommitTimeout(Duration.Zero))
      intercept[IllegalArgumentException](settings.withMaxProcessingTime(Duration.Zero))
      intercept[IllegalArgumentException](settings.withRenewalMargin(Duration.Zero))
      intercept[IllegalArgumentException](settings.withMaxAcknowledgementDelay((-1).second))
      intercept[IllegalArgumentException](settings.withMaxProcessingTime(366.days))
      intercept[IllegalArgumentException](settings.withMaxAcknowledgementDelay(366.days))
    }
  }

}
