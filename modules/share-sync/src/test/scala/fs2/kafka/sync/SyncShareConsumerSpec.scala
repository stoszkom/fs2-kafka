/*
 * Copyright 2018 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka.sync

import scala.concurrent.duration.*

import cats.effect.{IO, Ref}
import cats.effect.syntax.all.*
import cats.syntax.all.*
import fs2.concurrent.SignallingRef
import fs2.kafka.*
import fs2.Chunk

final class SyncShareConsumerSpec extends ShareConsumerBaseKafkaSpec {

  private type Records = Chunk[AcknowledgeableConsumerRecord[IO, String, String]]

  // Consumes the topic with `process` until it stopped the consumer.
  private def consumed(settings: ShareConsumerSettings[IO, String, String], topic: String)(
    process: SyncShareConsumer[IO, String, String] => Records => IO[Unit]
  ): IO[Unit] =
    SyncShareConsumer
      .resource(settings)
      .use(consumer => consumer.subscribeTo(topic) *> consumer.consume(process(consumer)))

  // Accepts records in parallel, and stops once `count` were accepted, returning values and
  // delivery counts.
  private def acceptAll(
    settings: ShareConsumerSettings[IO, String, String],
    topic: String,
    count: Int
  )(work: AcknowledgeableConsumerRecord[IO, String, String] => IO[Unit]): Vector[(String, Int)] =
    run(
      Ref
        .of[IO, Vector[(String, Int)]](Vector.empty)
        .flatMap { seen =>
          consumed(settings, topic) { consumer => records =>
            records
              .parTraverseN(8) { r =>
                work(r) *> r.accept *> seen
                  .updateAndGet(_ :+ (r.record.value -> r.offset.deliveryCount.toInt))
                  .flatMap(s => consumer.stopConsuming.whenA(s.size >= count))
              }
              .void
          } *> seen.get
        }
    )

  describe("SyncShareConsumer") {
    it("should accept every record, processed in parallel") {
      withTopic { topic =>
        createCustomTopic(topic, partitions = 2)
        val produced = (0 until 50).map(_.toString)
        publish(topic, produced*)
        val settings = shareConsumerSettings.withMaxPollRecords(10)
        val seen     = acceptAll(settings, topic, 50)(_ => IO.unit)
        seen.map(_._1) should contain theSameElementsAs produced
        assertSettled(topic, settings)
      }
    }

    it("should fail when not subscribed") {
      run(
        SyncShareConsumer.resource(shareConsumerSettings).use(_.consume(_ => IO.unit).attempt)
      ).left.toOption.get shouldBe a[NotSubscribedException]
    }

    it("should release the records process leaves unacknowledged") {
      withTopic { topic =>
        createCustomTopic(topic)
        publish(topic, "0")
        val settings = shareConsumerSettings
        run(consumed(settings, topic)(consumer => _ => consumer.stopConsuming))
        readUntil(topic, settings, "0") shouldBe Vector("0" -> 2)
      }
    }

    it("should release the records and raise the error when process fails") {
      withTopic { topic =>
        createCustomTopic(topic)
        publish(topic, "0")
        val settings = shareConsumerSettings
        val result   = run(
          SyncShareConsumer
            .resource(settings)
            .use(consumer =>
              consumer.subscribeTo(topic) *>
                consumer.consume(_ => IO.raiseError[Unit](new RuntimeException("boom"))).attempt
            )
        )
        result.left.toOption.map(_.getMessage) shouldBe Some("boom")
        readUntil(topic, settings, "0") shouldBe Vector("0" -> 2)
      }
    }

    it("should fail an acknowledgement made after process returned") {
      withTopic { topic =>
        createCustomTopic(topic)
        publish(topic, "0")
        val result = run(
          Ref
            .of[IO, Option[AcknowledgeableConsumerRecord[IO, String, String]]](None)
            .flatMap { kept =>
              SyncShareConsumer
                .resource(shareConsumerSettings)
                .use(consumer =>
                  consumer.subscribeTo(topic) *>
                    consumer.consume(records => kept.set(records.head) *> consumer.stopConsuming) *>
                    kept.get.flatMap(_.get.accept.attempt)
                )
            }
        )
        result.left.toOption.get shouldBe a[RecordNotInFlightException]
      }
    }

    it("should renew a record automatically while it is processed past its lock") {
      withTopic { topic =>
        createCustomTopic(topic)
        publish(topic, "0")
        val settings = shareConsumerSettings.withAutoRenewal(true)
        acceptAll(settings, topic, 1)(_ => IO.sleep(lock + 1.second)) shouldBe Vector("0" -> 1)
        assertSettled(topic, settings)
      }
    }

    it("should accept a record the user renewed while another record is slow") {
      withTopic { topic =>
        createCustomTopic(topic)
        publish(topic, "0", "1")
        val settings = shareConsumerSettings.withMaxPollRecords(2).withAutoRenewal(true)
        val seen     = acceptAll(settings, topic, 2) { r =>
          if (r.record.value == "0") r.renew *> IO.sleep(1.second) else IO.sleep(lock * 4 / 3)
        }
        seen should contain theSameElementsAs Vector("0" -> 1, "1" -> 1)
        assertSettled(topic, settings)
      }
    }

    // Records of a finished poll stay in the state while the next polls return nothing: a renewal
    // of one must fail rather than reach Java, whose poll would acquire records outside the loop.
    it("should fail a renewal made after process returned") {
      withTopic { topic =>
        createCustomTopic(topic)
        publish(topic, "0")
        val renewal = run(
          SignallingRef
            .of[IO, Option[AcknowledgeableConsumerRecord[IO, String, String]]](None)
            .flatMap { kept =>
              SyncShareConsumer
                .resource(shareConsumerSettings)
                .use { consumer =>
                  consumer.subscribeTo(topic) *>
                    consumer
                      .consume(_.traverse_(r => r.accept *> kept.set(Some(r))))
                      .background
                      .use(_ =>
                        kept.waitUntil(_.isDefined) *> IO.sleep(3.seconds) *>
                          kept.get.flatMap(_.get.renew.attempt)
                      )
                }
            }
        )
        renewal.left.toOption.get shouldBe a[RecordNotInFlightException]
      }
    }

    it("should release a record past the max processing time") {
      withTopic { topic =>
        createCustomTopic(topic)
        publish(topic, "0")
        val settings = shareConsumerSettings.withMaxProcessingTime(2.seconds)
        val late     = run(
          Ref
            .of[IO, Option[Either[Throwable, Unit]]](None)
            .flatMap { result =>
              consumed(settings, topic) { consumer => records =>
                records.traverse_(r =>
                  IO.sleep(4.seconds) *> r.accept.attempt.flatMap(a => result.set(Some(a)))
                ) *> consumer.stopConsuming
              } *> result.get
            }
        )
        late.flatMap(_.left.toOption).get shouldBe a[RecordNotInFlightException]
        readUntil(topic, settings, "0") shouldBe Vector("0" -> 2)
      }
    }

    it("should not poll while paused") {
      withTopic { topic =>
        createCustomTopic(topic)
        publish(topic, "0")
        val (whilePaused, after) = run(
          (SignallingRef[IO, Boolean](true), Ref.of[IO, Int](0)).flatMapN { (paused, polls) =>
            val settings = shareConsumerSettings.withPauseWhen(paused)
            SyncShareConsumer
              .resource(settings)
              .use { consumer =>
                consumer.subscribeTo(topic) *>
                  consumer
                    .consume(records =>
                      polls.update(_ + 1) *> records.traverse_(_.accept) *> consumer.stopConsuming
                    )
                    .background
                    .use(done =>
                      IO.sleep(10.seconds) *> polls
                        .get
                        .flatMap(before =>
                          paused.set(false) *> done.flatMap(_.embedNever) *> polls
                            .get
                            .tupleLeft(before)
                        )
                    )
              }
          }
        )
        (whilePaused, after) shouldBe ((0, 1))
      }
    }

    // As for the actor: renewing must not acquire the records published while paused.
    it("should not raise the delivery count of new records while renewing during a pause") {
      withTopic { topic =>
        createCustomTopic(topic)
        publish(topic, "0")
        val seen = run(
          (SignallingRef[IO, Boolean](false), Ref.of[IO, Vector[(String, Int)]](Vector.empty))
            .flatMapN { (paused, seen) =>
              val settings = shareConsumerSettings.withAutoRenewal(true).withPauseWhen(paused)
              SyncShareConsumer
                .resource(settings)
                .use { consumer =>
                  consumer.subscribeTo(topic) *>
                    consumer.consume(_.traverse_ { r =>
                      val arrived = r.record.value -> r.offset.deliveryCount.toInt
                      val held    =
                        if (r.record.value == "0")
                          paused.set(true) *> IO.blocking(publish(topic, "1", "2")) *>
                            IO.sleep(lock * 3) *> r.accept *> paused.set(false)
                        else r.accept
                      held *> seen
                        .updateAndGet(_ :+ arrived)
                        .flatMap(s => consumer.stopConsuming.whenA(s.size >= 3))
                    }) *> seen.get
                }
            }
        )
        seen should contain theSameElementsAs Vector("0" -> 1, "1" -> 1, "2" -> 1)
      }
    }

    it("should deliver a poll again after process failed in implicit mode, and refuse to go on") {
      withTopic { topic =>
        createCustomTopic(topic)
        publish(topic, "0")
        val settings       = shareConsumerSettings
        val (first, again) = run(
          SyncShareConsumer
            .resource(settings.withProperty("share.acknowledgement.mode", "implicit"))
            .use { consumer =>
              consumer.subscribeTo(topic) *>
                (
                  consumer
                    .autoConsume(_ => IO.raiseError[Unit](new RuntimeException("boom")))
                    .attempt,
                  consumer.autoConsume(_ => IO.unit).attempt
                ).tupled
            }
        )
        first.left.toOption.map(_.getMessage) shouldBe Some("boom")
        again.left.toOption.get shouldBe an[IllegalStateException]
        readUntil(topic, settings, "0") shouldBe Vector("0" -> 2)
      }
    }

    it("should settle every record once across several consumers") {
      withTopic { topic =>
        createCustomTopic(topic, partitions = 2)
        val produced = (0 until 40).map(_.toString)
        publish(topic, produced*)
        val settings = shareConsumerSettings.withMaxPollRecords(5)
        val seen     = run(
          Ref
            .of[IO, Vector[(String, Int)]](Vector.empty)
            .flatMap { seen =>
              SyncShareConsumers
                .resource(settings, 2)
                .use { consumers =>
                  consumers.subscribeTo(topic) *>
                    consumers.consume(_.traverse_ { r =>
                      r.accept *> seen
                        .updateAndGet(_ :+ (r.record.value -> r.offset.deliveryCount.toInt))
                        .flatMap(s => consumers.stopConsuming.whenA(s.size >= produced.size))
                    }) *> seen.get
                }
            }
        )
        seen.map(_._1) should contain theSameElementsAs produced
        assertSettled(topic, settings)
      }
    }

    it("should accept each poll in implicit mode") {
      withTopic { topic =>
        createCustomTopic(topic)
        publish(topic, "0", "1", "2")
        val settings = shareConsumerSettings
        val seen     = run(
          Ref
            .of[IO, Vector[String]](Vector.empty)
            .flatMap { seen =>
              SyncShareConsumer
                .resource(settings.withProperty("share.acknowledgement.mode", "implicit"))
                .use { consumer =>
                  consumer.subscribeTo(topic) *>
                    consumer.autoConsume(records =>
                      seen
                        .updateAndGet(_ ++ records.toVector.map(_.value))
                        .flatMap(s => consumer.stopConsuming.whenA(s.size >= 3))
                    ) *> seen.get
                }
            }
        )
        seen should contain theSameElementsAs Vector("0", "1", "2")
        assertSettled(topic, settings)
      }
    }
  }

}
