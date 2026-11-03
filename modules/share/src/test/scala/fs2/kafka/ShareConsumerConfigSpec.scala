/*
 * Copyright 2018 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import fs2.kafka.internal.actor.ShareConsumerConfig
import scala.concurrent.duration.*

final class ShareConsumerConfigSpec extends BaseSpec {

  private val settings = ShareConsumerSettings[IO, String, String]

  private def config(s: ShareConsumerSettings[IO, String, String]) =
    ShareConsumerConfig.from(s).unsafeRunSync()

  describe("ShareConsumerConfig") {
    it("should read the library's defaults") {
      val c = config(settings)
      assert(!c.implicitMode)
      assert(c.maxPollInterval == 5.minutes && c.apiTimeout == 1.minute)
    }

    it("should read the mode as Java does, and turn renewal off in implicit mode") {
      val c = config(
        settings.withAutoRenewal(true).withProperty("share.acknowledgement.mode", " implicit\n")
      )
      assert(c.implicitMode && !c.settings.autoRenewal)
    }

    it("should read the poll and API timeouts") {
      val c = config(
        settings
          .withProperty("max.poll.interval.ms", " 1000 ")
          .withProperty("default.api.timeout.ms", "2000")
      )
      assert(c.maxPollInterval == 1.second && c.apiTimeout == 2.seconds)
    }

    it("should renew the margin before the lock ends, or at half the lock") {
      val c = config(settings)
      assert(c.renewalInterval(Some(30.seconds)) == 25.seconds)
      assert(
        config(settings.withRenewalMargin(40.seconds)).renewalInterval(Some(30.seconds)) == 15.seconds
      )
      assert(c.renewalInterval(None) == ShareConsumerConfig.unknownLockRenewalInterval)
    }

    it("should renew within half of max.poll.interval.ms") {
      val c = config(settings.withProperty("max.poll.interval.ms", "20000"))
      assert(c.renewalInterval(Some(30.seconds)) == 10.seconds)
    }

    it("should wait for a renewal up to the margin, and at most half the lock") {
      val c = config(settings)
      assert(c.renewalWait(Some(30.seconds)) == 5.seconds)
      assert(c.renewalWait(Some(6.seconds)) == 3.seconds)
      assert(c.renewalWait(None) == 5.seconds)
    }
  }

}
