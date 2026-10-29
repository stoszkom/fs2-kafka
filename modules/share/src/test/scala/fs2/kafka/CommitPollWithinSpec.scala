/*
 * Copyright 2018 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka

import java.util.concurrent.ConcurrentLinkedQueue

import scala.concurrent.duration.*

import cats.effect.testkit.TestControl
import cats.effect.unsafe.implicits.global
import cats.effect.IO
import fs2.kafka.internal.converters.collection.*
import fs2.kafka.ShareTestCommitters.*
import fs2.Stream

final class CommitPollWithinSpec extends BaseSpec {

  describe("commitPollWithin") {
    it("should commit within maxDelay, even when the deadline passes while another is committed") {
      val committed    = new ConcurrentLinkedQueue[List[Long]]
      val (slow, fast) = (committer(committed, 200.millis), committer(committed))
      TestControl
        .executeEmbed(
          (Stream.emit(acknowledged(slow, 1L)) ++ Stream.sleep_[IO](30.millis) ++
            Stream.emit(acknowledged(fast, 2L)) ++ Stream.never[IO])
            .through(commitPollWithin(50.millis))
            .take(2)
            .compile
            .drain
        )
        .unsafeRunSync()
      committed.asScala.toList shouldBe List(List(1L), List(2L))
    }

    it("should commit the polls of several consumers completed together in parallel") {
      val committed       = new ConcurrentLinkedQueue[List[Long]]
      val (first, second) =
        (
          committer(committed, 200.millis, pending = 1),
          committer(committed, 200.millis, pending = 1)
        )
      val elapsed =
        TestControl
          .executeEmbed(
            (Stream(acknowledged(first, 1L), acknowledged(second, 2L)) ++ Stream.never[IO])
              .through(commitPollWithin(1.minute))
              .take(1)
              .compile
              .drain
              .timed
              .map(_._1)
          )
          .unsafeRunSync()
      elapsed shouldBe 200.millis
      committed.asScala.toSet shouldBe Set(List(1L), List(2L))
    }

    it("should count several offsets of one record once") {
      val committed = new ConcurrentLinkedQueue[List[Long]]
      val c         = committer(committed, pending = 2)
      val elapsed   =
        TestControl
          .executeEmbed(
            (Stream(acknowledged(c, 1L), acknowledged(c, 1L)) ++ Stream.never[IO])
              .through(commitPollWithin(300.millis))
              .take(1)
              .compile
              .drain
              .timed
              .map(_._1)
          )
          .unsafeRunSync()
      elapsed shouldBe 300.millis
    }

    it("should commit what is left when the input ends") {
      val committed = new ConcurrentLinkedQueue[List[Long]]
      val c         = committer(committed)
      TestControl
        .executeEmbed(
          Stream(acknowledged(c, 1L), acknowledged(c, 2L))
            .covary[IO]
            .through(commitPollWithin(1.minute))
            .compile
            .drain
        )
        .unsafeRunSync()
      committed.asScala.toList shouldBe List(List(1L, 2L))
    }
  }

}
