/*
 * Copyright 2018 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka

import java.util.concurrent.ConcurrentLinkedQueue

import cats.~>
import cats.data.OptionT
import cats.effect.unsafe.implicits.global
import cats.effect.IO
import fs2.kafka.internal.converters.collection.*
import fs2.kafka.ShareTestCommitters.*

final class AcknowledgedOffsetSpec extends BaseSpec {

  describe("AcknowledgedOffset") {
    it("should keep offsets of the same committer batchable after mapK") {
      val committed = new ConcurrentLinkedQueue[List[Long]]
      val c         = committer(committed)
      val f         = new (IO ~> OptionT[IO, *]) {
        override def apply[A](fa: IO[A]): OptionT[IO, A] = OptionT.liftF(fa)
      }
      val offsets = List(1L, 2L).map(acknowledged(c, _).mapK(f))
      offsets.map(_.committer).distinct.size shouldBe 1
      val batch = AcknowledgedOffsetBatch.fromFoldable(offsets)
      batch.offsets.size shouldBe 1
      batch.commit.value.unsafeRunSync()
      committed.asScala.toList shouldBe List(List(1L, 2L))
    }
  }

}
