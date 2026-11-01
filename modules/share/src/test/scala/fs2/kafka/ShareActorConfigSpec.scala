/*
 * Copyright 2018 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka

import scala.concurrent.duration.*

import cats.effect.unsafe.implicits.global
import cats.effect.IO
import fs2.kafka.internal.actor.ShareActorConfig

final class ShareActorConfigSpec extends BaseSpec {

  private val settings = ShareConsumerSettings[IO, String, String]

  private def config(s: ShareConsumerSettings[IO, String, String]) =
    ShareActorConfig.from(s).unsafeRunSync()

  describe("ShareActorConfig") {
    it("should read the library's defaults") {
      val c = config(settings)
      assert(!c.implicitMode && !c.batchOptimized && c.maxPollRecords == 500)
      assert(c.maxPollInterval == 5.minutes && c.apiTimeout == 1.minute)
    }

    it("should read the modes in any case, and turn renewal off in implicit mode") {
      val c = config(
        settings
          .withAutoRenewal(true)
          .withProperty("share.acknowledgement.mode", "IMPLICIT")
          .withProperty("share.acquire.mode", "Batch_Optimized")
      )
      assert(c.implicitMode && c.batchOptimized && !c.settings.autoRenewal)
    }

    it("should read the poll and API timeouts") {
      val c = config(
        settings
          .withMaxPollRecords(7)
          .withProperty("max.poll.interval.ms", "1000")
          .withProperty("default.api.timeout.ms", "2000")
      )
      assert(c.maxPollRecords == 7 && c.maxPollInterval == 1.second && c.apiTimeout == 2.seconds)
    }
  }

}
