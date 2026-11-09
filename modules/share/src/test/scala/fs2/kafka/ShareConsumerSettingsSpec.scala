/*
 * Copyright 2018 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka

import cats.effect.IO
import cats.syntax.all.*
import scala.concurrent.duration.*

final class ShareConsumerSettingsSpec extends BaseSpec {

  private val settings = ShareConsumerSettings[IO, String, String]

  describe("ShareConsumerSettings") {
    it("should default to explicit acknowledgement, record_limit acquisition and no renewal") {
      settings.properties shouldBe Map(
        "share.acknowledgement.mode" -> "explicit",
        "share.acquire.mode" -> "record_limit"
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
      settings.failureAcknowledgement shouldBe AcknowledgeType.Release
      settings
        .withFailureAcknowledgement(AcknowledgeType.Reject)
        .failureAcknowledgement shouldBe AcknowledgeType.Reject
      settings.maxAcknowledgementDelay shouldBe Some(1.second)
      settings.withMaxAcknowledgementDelay(Duration.Zero).maxAcknowledgementDelay shouldBe
        Some(Duration.Zero)
      settings.withoutMaxAcknowledgementDelay.maxAcknowledgementDelay shouldBe None
    }

    it("should default the timeouts as KafkaConsumer's settings do") {
      settings.closeTimeout shouldBe 20.seconds
      settings.pollTimeout shouldBe 50.millis
      settings.commitTimeout shouldBe 15.seconds
      settings.withCloseTimeout(1.second).closeTimeout shouldBe 1.second
      settings.withPollTimeout(1.second).pollTimeout shouldBe 1.second
      settings.withCommitTimeout(1.second).commitTimeout shouldBe 1.second
    }

    it("should set the Java properties") {
      settings
        .withBootstrapServers("localhost:9092")
        .withGroupId("group")
        .withMaxPollRecords(10)
        .withMaxPollInterval(1.minute)
        .properties shouldBe settings.properties ++ Map(
        "bootstrap.servers" -> "localhost:9092",
        "group.id" -> "group",
        "max.poll.records" -> "10",
        "max.poll.interval.ms" -> "60000"
      )
    }

    it("should show as its toString") {
      settings.show shouldBe settings.toString
    }

    it("should set the acknowledgement and acquire modes") {
      settings
        .withAcknowledgementMode(ShareAcknowledgementMode.Implicit)
        .withAcquireMode(ShareAcquireMode.BatchOptimized)
        .properties shouldBe Map(
        "share.acknowledgement.mode" -> "implicit",
        "share.acquire.mode" -> "batch_optimized"
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
      intercept[IllegalArgumentException](settings.withFailureAcknowledgement(AcknowledgeType.Accept))
      intercept[IllegalArgumentException](settings.withFailureAcknowledgement(AcknowledgeType.Renew))
    }
  }

}
