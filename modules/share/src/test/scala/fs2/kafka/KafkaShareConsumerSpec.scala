/*
 * Copyright 2018 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka

import cats.effect.Deferred
import cats.effect.IO
import cats.effect.Ref
import cats.effect.syntax.all.*
import cats.syntax.all.*
import cats.~>
import fs2.Chunk
import fs2.Stream
import fs2.concurrent.SignallingRef
import org.apache.kafka.common.config.ConfigException
import scala.collection.immutable.SortedSet
import scala.concurrent.duration.*

final class KafkaShareConsumerSpec extends ShareConsumerBaseKafkaSpec {

  private type Records = Chunk[AcknowledgeableConsumerRecord[IO, String, String]]

  // Consumes the topic with `process` until it stopped the consumer.
  private def consumed(settings: ShareConsumerSettings[IO, String, String], topic: String)(
    process: KafkaShareConsumer[IO, String, String] => Records => IO[Unit]
  ): IO[Unit] =
    KafkaShareConsumer
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

  describe("KafkaShareConsumer") {
    it("should accept every record, processed in parallel") {
      withTopic { topic =>
        createTopic(topic, partitions = 2)
        val produced = (0 until 50).map(_.toString)
        publish(topic, produced*)
        val settings = shareConsumerSettings.withMaxPollRecords(10)
        val seen = acceptAll(settings, topic, 50)(_ => IO.unit)
        seen.map(_._1) should contain theSameElementsAs produced
        assertSettled(topic, settings)
      }
    }

    it("should fail when not subscribed") {
      run(
        KafkaShareConsumer.resource(shareConsumerSettings).use(_.consume(_ => IO.unit).attempt)
      ).left.toOption.get shouldBe a[NotSubscribedException]
    }

    it("should release the records process leaves unacknowledged") {
      withTopic { topic =>
        createTopic(topic)
        publish(topic, "0")
        val settings = shareConsumerSettings
        run(consumed(settings, topic)(consumer => _ => consumer.stopConsuming))
        readUntil(topic, settings, "0") shouldBe Vector("0" -> 2)
      }
    }

    it("should release the records and raise the error when process fails") {
      withTopic { topic =>
        createTopic(topic)
        publish(topic, "0")
        val settings = shareConsumerSettings
        val result = run(
          KafkaShareConsumer
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

    it("should release the poll and raise the error when a record fails to deserialize") {
      withTopic { topic =>
        createTopic(topic)
        publish(topic, "bad")
        val (first, second) =
          run(
            Ref
              .of[IO, Boolean](false)
              .flatMap { failed =>
                // Fails the first time only, so that the record can be consumed again.
                val values = Deserializer.lift[IO, String] { bytes =>
                  failed
                    .getAndSet(true)
                    .ifM(IO.pure(new String(bytes)), IO.raiseError(new RuntimeException("failed")))
                }
                val settings = ShareConsumerSettings(Deserializer[IO, String], values)
                  .withProperties(shareConsumerSettings.properties)
                KafkaShareConsumer
                  .resource(settings)
                  .use { consumer =>
                    val once =
                      consumer.consume(records => records.traverse_(_.accept) *> consumer.stopConsuming)
                    consumer.subscribeTo(topic) *> once.attempt
                      .flatMap { first =>
                        Ref
                          .of[IO, Vector[Int]](Vector.empty)
                          .flatMap { counts =>
                            consumer
                              .consume(records =>
                                counts.update(
                                  _ ++ records.toVector.map(_.offset.deliveryCount.toInt)
                                ) *>
                                  records.traverse_(_.accept) *> consumer.stopConsuming
                              )
                              .attempt
                              .flatMap(result => counts.get.map(result -> _))
                          }
                          .tupleLeft(first)
                      }
                  }
              }
          )
        first.left.toOption.map(_.getMessage) shouldBe Some("failed")
        second shouldBe (Right(()) -> Vector(2))
      }
    }

    // No poll follows a failed process, so the releases are sent at once: the record is delivered
    // again well before its lock ends, while the consumer stays open.
    it("should send the releases at once when process fails") {
      withTopic { topic =>
        createTopic(topic)
        publish(topic, "0")
        val settings = shareConsumerSettings
        val (elapsed, again) = run(
          KafkaShareConsumer
            .resource(settings)
            .use { consumer =>
              consumer.subscribeTo(topic) *>
                consumer.consume(_ => IO.raiseError[Unit](new RuntimeException("boom"))).attempt *>
                IO.blocking(readUntil(topic, settings, "0")).timed
            }
        )
        elapsed should be < lock / 2
        again shouldBe Vector("0" -> 2)
      }
    }

    it("should fail to consume again once stopped") {
      withTopic { topic =>
        createTopic(topic)
        publish(topic, "0")
        val again = run(
          KafkaShareConsumer
            .resource(shareConsumerSettings)
            .use { consumer =>
              consumer.subscribeTo(topic) *>
                consumer.consume(_.traverse_(_.accept) *> consumer.stopConsuming) *>
                consumer.consume(_ => IO.unit).attempt
            }
        )
        again.left.toOption.map(_.getMessage) shouldBe Some("the consumer was stopped")
      }
    }

    it("should fail an acknowledgement made after process returned") {
      withTopic { topic =>
        createTopic(topic)
        publish(topic, "0")
        val result = run(
          Ref
            .of[IO, Option[AcknowledgeableConsumerRecord[IO, String, String]]](None)
            .flatMap { kept =>
              KafkaShareConsumer
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
        createTopic(topic)
        publish(topic, "0")
        val settings = shareConsumerSettings.withAutoRenewal(true)
        acceptAll(settings, topic, 1)(_ => IO.sleep(lock + 1.second)) shouldBe Vector("0" -> 1)
        assertSettled(topic, settings)
      }
    }

    it("should accept a record the user renewed while another record is slow") {
      withTopic { topic =>
        createTopic(topic)
        publish(topic, "0", "1")
        val settings = shareConsumerSettings.withMaxPollRecords(2).withAutoRenewal(true)
        val seen = acceptAll(settings, topic, 2) { r =>
          if (r.record.value == "0") r.renew *> IO.sleep(1.second) else IO.sleep(lock * 4 / 3)
        }
        seen should contain theSameElementsAs Vector("0" -> 1, "1" -> 1)
        assertSettled(topic, settings)
      }
    }

    // Java completes a renewal which outlasted its wait later, and its next poll returns the record
    // with the same delivery count: it is released at once rather than handed out again, as its
    // later acknowledgements have failed already.
    it("should release a record returned by a renewal which outlasted its wait") {
      withTopic { topic =>
        createTopic(topic)
        publish(topic, "0")
        // The wait ends before the broker can answer, so the renewal is lost.
        val settings = shareConsumerSettings.withRenewalMargin(1.nanosecond)
        val (seen, lateAccept) = run(
          (
            Ref.of[IO, Vector[(String, Int)]](Vector.empty),
            Ref.of[IO, Option[Either[Throwable, Unit]]](None)
          ).flatMapN { (seen, lateAccept) =>
            consumed(settings, topic) { consumer => records =>
              records.traverse_ { r =>
                seen
                  .updateAndGet(_ :+ (r.record.value -> r.offset.deliveryCount.toInt))
                  .flatMap { s =>
                    if (s.size == 1)
                      r.renew *> r.accept.attempt.flatMap(a => lateAccept.set(Some(a)))
                    else r.accept *> consumer.stopConsuming
                  }
              }
            } *> (seen.get, lateAccept.get).tupled
          }
        )
        seen shouldBe Vector("0" -> 1, "0" -> 2)
        lateAccept.flatMap(_.left.toOption).get shouldBe a[RecordNotInFlightException]
        assertSettled(topic, settings)
      }
    }

    // Records of a finished poll stay in the state while the next polls return nothing: a renewal
    // of one must fail rather than reach Java, whose poll would acquire records outside the loop.
    it("should fail a renewal made after process returned") {
      withTopic { topic =>
        createTopic(topic)
        publish(topic, "0")
        val renewal = run(
          SignallingRef
            .of[IO, Option[AcknowledgeableConsumerRecord[IO, String, String]]](None)
            .flatMap { kept =>
              KafkaShareConsumer
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
        createTopic(topic)
        publish(topic, "0")
        val settings = shareConsumerSettings.withMaxProcessingTime(2.seconds)
        val late = run(
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
        // Java fetches in the background while process runs, so it may take the released record into
        // its buffer and release it when closing: the delivery count is 2 or 3.
        readUntil(topic, settings, "0").map(_._1) shouldBe Vector("0")
      }
    }

    it("should not poll while paused") {
      withTopic { topic =>
        createTopic(topic)
        publish(topic, "0")
        val (whilePaused, after) = run(
          (SignallingRef[IO, Boolean](true), Ref.of[IO, Int](0)).flatMapN { (paused, polls) =>
            val settings = shareConsumerSettings.withPauseWhen(paused)
            KafkaShareConsumer
              .resource(settings)
              .use { consumer =>
                consumer.subscribeTo(topic) *>
                  consumer
                    .consume(records =>
                      polls.update(_ + 1) *> records.traverse_(_.accept) *> consumer.stopConsuming
                    )
                    .background
                    .use(done =>
                      IO.sleep(10.seconds) *> polls.get
                        .flatMap(before =>
                          paused.set(false) *> done.flatMap(_.embedNever) *> polls.get
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
        createTopic(topic)
        publish(topic, "0")
        val seen = run(
          (SignallingRef[IO, Boolean](false), Ref.of[IO, Vector[(String, Int)]](Vector.empty))
            .flatMapN { (paused, seen) =>
              val settings = shareConsumerSettings.withAutoRenewal(true).withPauseWhen(paused)
              KafkaShareConsumer
                .resource(settings)
                .use { consumer =>
                  consumer.subscribeTo(topic) *>
                    consumer.consume(_.traverse_ { r =>
                      val arrived = r.record.value -> r.offset.deliveryCount.toInt
                      val held =
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
        createTopic(topic)
        publish(topic, "0")
        val settings = shareConsumerSettings
        val (first, again) = run(
          KafkaShareConsumer
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
        createTopic(topic, partitions = 2)
        val produced = (0 until 40).map(_.toString)
        publish(topic, produced*)
        val settings = shareConsumerSettings.withMaxPollRecords(5)
        val seen = run(
          Ref
            .of[IO, Vector[(String, Int)]](Vector.empty)
            .flatMap { seen =>
              KafkaShareConsumers
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
        createTopic(topic)
        publish(topic, "0", "1", "2")
        val settings = shareConsumerSettings
        val seen = run(
          Ref
            .of[IO, Vector[String]](Vector.empty)
            .flatMap { seen =>
              KafkaShareConsumer
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

  // Runs `process` on the first poll only, then stops, returning its result.
  private def firstPoll[A](settings: ShareConsumerSettings[IO, String, String], topic: String)(
    process: Records => IO[A]
  ): IO[A] =
    Ref
      .of[IO, Option[A]](None)
      .flatMap { result =>
        consumed(settings, topic) { consumer => records =>
          process(records).flatMap(a => result.set(Some(a))) *> consumer.stopConsuming
        } *> result.get.map(_.get)
      }

  // Handles each record with `handle`, one at a time, and stops once `count` were handled,
  // returning values and delivery counts in the order handled.
  private def handled(
    settings: ShareConsumerSettings[IO, String, String],
    topic: String,
    count: Int
  )(
    handle: AcknowledgeableConsumerRecord[IO, String, String] => IO[Unit]
  ): IO[Vector[(String, Int)]] =
    Ref
      .of[IO, Vector[(String, Int)]](Vector.empty)
      .flatMap { seen =>
        consumed(settings, topic) { consumer => records =>
          records.traverse_ { r =>
            handle(r) *> seen
              .updateAndGet(_ :+ (r.record.value -> r.offset.deliveryCount.toInt))
              .flatMap(s => consumer.stopConsuming.whenA(s.size >= count))
          }
        } *> seen.get
      }

  private val accepting: AcknowledgeType = AcknowledgeType.Accept

  describe("creating share consumers") {
    it("should fail for an unknown acknowledgement mode, as Java's consumer does") {
      val result = run(
        KafkaShareConsumer
          .resource(shareConsumerSettings.withProperty("share.acknowledgement.mode", "manual"))
          .use_
          .attempt
      )
      result.swap.toOption.get shouldBe a[ConfigException]
    }

    it("should accept share.acquire.mode in any case, as Kafka does") {
      run(
        KafkaShareConsumer
          .resource(shareConsumerSettings.withProperty("share.acquire.mode", "RECORD_LIMIT"))
          .use_
      )
    }

    it("should report the subscription and metrics") {
      withTopic { topic =>
        createTopic(topic)
        publish(topic, "0")
        val (subscription, metrics) = run(
          KafkaShareConsumer
            .resource(shareConsumerSettings)
            .use { consumer =>
              consumer.subscribeTo(topic) *>
                consumer.consume(_.traverse_(_.accept) *> consumer.stopConsuming) *>
                (consumer.subscription, consumer.metrics).tupled
            }
        )
        subscription shouldBe SortedSet(topic)
        metrics should not be empty
      }
    }

    it("should keep consuming across unsubscribe and a new subscription") {
      withTopic { first =>
        withTopic { second =>
          createTopic(first)
          createTopic(second)
          publish(first, "first")
          publish(second, "second")
          val consumed = run(
            Ref
              .of[IO, Vector[String]](Vector.empty)
              .flatMap { seen =>
                KafkaShareConsumer
                  .resource(shareConsumerSettings)
                  .use { consumer =>
                    consumer.subscribeTo(first) *>
                      consumer.consume(_.traverse_ { r =>
                        r.accept *>
                          (consumer.unsubscribe *> consumer.subscribeTo(second)).whenA(
                            r.record.value == "first"
                          ) *>
                          seen
                            .updateAndGet(_ :+ r.record.value)
                            .flatMap(s => consumer.stopConsuming.whenA(s.size >= 2))
                      }) *> seen.get
                  }
              }
          )
          consumed shouldBe Vector("first", "second")
        }
      }
    }

    it("should fail consume in implicit mode, and autoConsume in explicit mode") {
      val implicitly = shareConsumerSettings.withProperty("share.acknowledgement.mode", "implicit")
      val (explicitResult, implicitResult) = run(
        (
          KafkaShareConsumer.resource(implicitly).use(_.consume(_ => IO.unit).attempt),
          KafkaShareConsumer.resource(shareConsumerSettings).use(_.autoConsume(_ => IO.unit).attempt)
        ).tupled
      )
      explicitResult.swap.toOption.get shouldBe an[ImplicitAcknowledgementException]
      implicitResult.swap.toOption.get shouldBe an[IllegalStateException]
    }
  }

  describe("delivery") {
    it("should not deliver accepted or rejected records again") {
      withTopic { topic =>
        createTopic(topic)
        publish(topic, "0", "1")
        val settings = shareConsumerSettings
        run(handled(settings, topic, 2)(r => if (r.record.value == "0") r.accept else r.reject))
        assertSettled(topic, settings)
      }
    }

    it("should deliver released records again, with a higher delivery count") {
      withTopic { topic =>
        createTopic(topic)
        publish(topic, "0")
        val deliveries = run(
          handled(shareConsumerSettings, topic, 2)(r =>
            if (r.offset.deliveryCount == 1) r.release else r.accept
          )
        )
        deliveries shouldBe Vector("0" -> 1, "0" -> 2)
      }
    }

    it("should let two consumers process records of the same partition concurrently") {
      withTopic { topic =>
        createTopic(topic)
        publish(topic, "0", "1")
        val settings = shareConsumerSettings.withMaxPollRecords(1)
        val consumed = run(
          (Deferred[IO, Unit], Ref.of[IO, Int](0)).flatMapN { (bothStarted, started) =>
            val consume = firstPoll(settings, topic)(_.traverse { r =>
              started.updateAndGet(_ + 1).flatMap(n => bothStarted.complete(()).whenA(n == 2)) *>
                bothStarted.get *> r.accept.as(r.record.value)
            })
            IO.both(consume, consume)
          }
        )
        (consumed._1 ++ consumed._2).toList.toSet shouldBe Set("0", "1")
      }
    }

    it("should settle every record once with batch_optimized acquisition") {
      withTopic { topic =>
        createTopic(topic)
        val produced = (0 until 20).map(_.toString)
        publish(topic, produced*)
        val settings = shareConsumerSettings
          .withProperty("share.acquire.mode", "batch_optimized")
          .withMaxPollRecords(2)
        val consumed = run(handled(settings, topic, produced.size)(_.accept))
        consumed.map(_._1) should contain theSameElementsAs produced
        all(consumed.map(_._2)) shouldBe 1
      }
    }
  }

  describe("acknowledging records") {
    // The second record stays unacknowledged past its lock, and no poll runs while process does,
    // so only the delay sends the first's acknowledgement in time.
    it("should send pending acknowledgements after the max acknowledgement delay, without a poll") {
      withTopic { topic =>
        createTopic(topic)
        publish(topic, "0", "1")
        val settings = shareConsumerSettings.withMaxPollRecords(2)
        run(
          firstPoll(settings.withMaxAcknowledgementDelay(100.millis), topic) { records =>
            records.head.get.accept *> IO.sleep(lock + 1.second) *>
              records.drop(1).traverse_(_.release.attempt)
          }
        )
        publish(topic, "after")
        readUntil(topic, settings, "after").map(_._1) shouldBe Vector("1", "after")
      }
    }

    it("should send pending acknowledgements with the next poll, and the last ones when stopping") {
      withTopic { topic =>
        createTopic(topic)
        publish(topic, (0 until 10).map(_.toString)*)
        val settings = shareConsumerSettings.withMaxPollRecords(5).withoutMaxAcknowledgementDelay
        run(
          Ref
            .of[IO, Int](0)
            .flatMap { count =>
              consumed(settings, topic) { consumer => records =>
                Stream
                  .chunk(records)
                  .map(_.offset -> accepting)
                  .through(acknowledgeChunks[IO])
                  .compile
                  .drain *> count
                  .updateAndGet(_ + records.size)
                  .flatMap(n => consumer.stopConsuming.whenA(n >= 10))
              }
            }
        )
        assertSettled(topic, settings)
      }
    }

    it("should let the last of several acknowledgements made together for a record win") {
      withTopic { topic =>
        createTopic(topic)
        publish(topic, "0")
        val settings = shareConsumerSettings
        run(
          firstPoll(settings, topic) { records =>
            Stream
              .chunk(records)
              .flatMap(r =>
                Stream(
                  r.offset -> accepting,
                  r.offset -> (AcknowledgeType.Release: AcknowledgeType)
                )
              )
              .through(acknowledgeChunks[IO])
              .compile
              .drain
          }
        )
        val again = readUntil(topic, settings, "0")
        again.map(_._1) shouldBe Vector("0")
        again.head._2 should be >= 2
      }
    }

    it("should fail an acknowledgement for a delivery whose acknowledgement was sent") {
      withTopic { topic =>
        createTopic(topic)
        publish(topic, "0")
        // Without a delay, the acknowledgement is sent at once.
        val late = run(
          firstPoll(shareConsumerSettings.withMaxAcknowledgementDelay(Duration.Zero), topic)(
            _.traverse(r => r.accept *> IO.sleep(1.second) *> r.reject.attempt).map(_.head.get)
          )
        )
        late.swap.toOption.get shouldBe a[RecordNotInFlightException]
      }
    }

    it("should fail an acknowledgement through the offset of an earlier delivery") {
      withTopic { topic =>
        createTopic(topic)
        publish(topic, "0")
        val stale = run(
          (
            Ref.of[IO, Option[AcknowledgeableOffset[IO]]](None),
            Ref.of[IO, Option[Either[Throwable, Unit]]](None)
          ).flatMapN { (earlier, result) =>
            consumed(shareConsumerSettings, topic) { consumer => records =>
              records.traverse_ { r =>
                earlier.get
                  .flatMap {
                    case None => earlier.set(Some(r.offset)) *> r.release
                    case Some(first) =>
                      first.accept.attempt.flatMap(a => result.set(Some(a))) *> r.accept *>
                        consumer.stopConsuming
                  }
              }
            } *> result.get.map(_.get)
          }
        )
        stale.swap.toOption.get shouldBe a[RecordNotInFlightException]
      }
    }
  }

  describe("acknowledgeChunks") {
    it("should acknowledge offsets mapped with different functions through each") {
      withTopic { topic =>
        createTopic(topic)
        publish(topic, "0", "1")
        val calls = run(
          (Ref.of[IO, Set[String]](Set.empty), Ref.of[IO, Int](0)).flatMapN { (called, count) =>
            def tagged(tag: String): IO ~> IO = new (IO ~> IO) {
              def apply[A](io: IO[A]): IO[A] = called.update(_ + tag) *> io
            }
            val (first, second) = (tagged("first"), tagged("second"))
            consumed(shareConsumerSettings, topic) { consumer => records =>
              Stream
                .chunk(records)
                .map { r =>
                  val f = if (r.record.value == "0") first else second
                  r.offset.mapK(f) -> accepting
                }
                .through(acknowledgeChunks[IO])
                .compile
                .drain *> count
                .updateAndGet(_ + records.size)
                .flatMap(n => consumer.stopConsuming.whenA(n >= 2))
            } *> called.get
          }
        )
        calls shouldBe Set("first", "second")
      }
    }

    it("should keep the last acknowledgement of a record reached through several acknowledgers") {
      withTopic { topic =>
        createTopic(topic)
        publish(topic, "0", "1")
        // The first acknowledgement is slower, so that it would win if both were sent concurrently.
        val slow = new (IO ~> IO) {
          def apply[A](io: IO[A]): IO[A] = IO.sleep(1.second) *> io
        }
        val settings = shareConsumerSettings.withMaxPollRecords(2).withoutMaxAcknowledgementDelay
        val seen = run(
          Ref
            .of[IO, Vector[(String, Int)]](Vector.empty)
            .flatMap { seen =>
              consumed(settings, topic) { consumer => records =>
                seen.update(
                  _ ++ records.toVector.map(r => r.record.value -> r.offset.deliveryCount.toInt)
                ) *>
                  Stream
                    .chunk(records)
                    .flatMap { r =>
                      val o = r.offset
                      // The release replaces the accept, so "0" is delivered again.
                      if (r.record.value == "0" && o.deliveryCount == 1)
                        Stream.chunk(
                          Chunk[(AcknowledgeableOffset[IO], AcknowledgeType)](
                            o.mapK(slow) -> AcknowledgeType.Accept,
                            o -> AcknowledgeType.Release
                          )
                        )
                      else Stream(o -> accepting)
                    }
                    .through(acknowledgeChunks[IO])
                    .compile
                    .drain *> seen.get.flatMap(s => consumer.stopConsuming.whenA(s.size >= 3))
              } *> seen.get
            }
        )
        seen should contain theSameElementsAs Vector("0" -> 1, "1" -> 1, "0" -> 2)
      }
    }

    it("should settle every record once") {
      withTopic { topic =>
        createTopic(topic, partitions = 2)
        val produced = (0 until 30).map(_.toString)
        publish(topic, produced*)
        val consumed = run(
          Ref
            .of[IO, Vector[(String, Int)]](Vector.empty)
            .flatMap { seen =>
              this.consumed(shareConsumerSettings.withMaxPollRecords(5), topic) { consumer => records =>
                Stream
                  .chunk(records)
                  .covary[IO]
                  .parEvalMapUnordered(4) { r =>
                    seen
                      .update(_ :+ (r.record.value -> r.offset.deliveryCount.toInt))
                      .as(r.offset -> accepting)
                  }
                  .through(acknowledgeChunks[IO])
                  .compile
                  .drain *> seen.get
                  .flatMap(s => consumer.stopConsuming.whenA(s.size >= produced.size))
              } *> seen.get
            }
        )
        consumed.map(_._1) should contain theSameElementsAs produced
        all(consumed.map(_._2)) shouldBe 1
      }
    }

    it("should send a chunk's acknowledgements to each of its consumers") {
      withTopic { topic =>
        val other = topic + "-other"
        createTopic(topic)
        createTopic(other)
        publish(topic, "a")
        publish(other, "b")
        val settings = shareConsumerSettings
        run(
          (Deferred[IO, Records], Deferred[IO, Records], Deferred[IO, Unit]).flatMapN {
            (fromFirst, fromSecond, acknowledged) =>
              // Each consumer hands its poll out and keeps processing it until both are acknowledged.
              def holding(t: String, out: Deferred[IO, Records]) =
                firstPoll(settings, t)(records => out.complete(records) *> acknowledged.get)
              val acknowledgeBoth =
                (fromFirst.get, fromSecond.get).flatMapN { (a, b) =>
                  Stream
                    .chunk(a ++ b)
                    .map(_.offset -> accepting)
                    .through(acknowledgeChunks[IO])
                    .compile
                    .drain *> acknowledged.complete(()).void
                }
              (holding(topic, fromFirst), holding(other, fromSecond), acknowledgeBoth).parTupled.void
          }
        )
        publish(topic, "after")
        publish(other, "after")
        readUntil(topic, settings, "after") shouldBe Vector("after" -> 1)
        readUntil(other, settings, "after") shouldBe Vector("after" -> 1)
      }
    }
  }

  describe("renewal") {
    it("should acknowledge a record right after a renewal") {
      withTopic { topic =>
        createTopic(topic)
        publish(topic, "0")
        val settings = shareConsumerSettings
        run(firstPoll(settings, topic)(_.traverse_(r => r.renew *> r.accept)))
        assertSettled(topic, settings)
      }
    }

    it("should finish stopping after an unsubscribe while a renewal is pending") {
      withTopic { topic =>
        createTopic(topic)
        publish(topic, "0", "1")
        run(
          consumed(shareConsumerSettings.withMaxPollRecords(2), topic) { consumer => records =>
            records
              .parTraverseN(2) { r =>
                if (r.record.value == "0") r.renew
                else IO.sleep(1.second) *> consumer.unsubscribe *> r.accept.attempt.void
              }
              .void *> consumer.stopConsuming
          }.timeout(30.seconds)
        )
      }
    }

    it("should count the max processing time from the user's latest renewal") {
      withTopic { topic =>
        createTopic(topic)
        publish(topic, "0")
        val settings = shareConsumerSettings
        val result = run(
          firstPoll(settings.withMaxProcessingTime(3.seconds), topic)(
            _.traverse(r => IO.sleep(2.seconds) *> r.renew *> IO.sleep(2.seconds) *> r.accept.attempt)
          )
        )
        result.toList shouldBe List(Right(()))
        assertSettled(topic, settings)
      }
    }

    it("should keep a record past its lock with manual renewals only") {
      withTopic { topic =>
        createTopic(topic)
        publish(topic, "0")
        val settings = shareConsumerSettings
        run(
          firstPoll(settings.withAutoRenewal(false), topic)(
            _.traverse_(r => (IO.sleep(lock / 3) *> r.renew).replicateA_(4) *> r.accept)
          )
        )
        assertSettled(topic, settings)
      }
    }

    // The consumer does not track lock expiry itself: the acknowledgement is accepted, and the broker
    // rejects it once sent.
    it("should lose a record past its lock without renewal") {
      withTopic { topic =>
        createTopic(topic)
        publish(topic, "0", "1")
        val settings = shareConsumerSettings.withMaxPollRecords(2)
        val late = run(
          firstPoll(settings.withAutoRenewal(false), topic)(records =>
            IO.sleep(lock + 1.second) *> records.traverse(_.accept.attempt)
          )
        )
        late.toList.distinct shouldBe List(Right(()))
        all(readUntil(topic, settings, "1").map(_._2)) should be >= 2
      }
    }

    it("should accept a renewed record without waiting for the next renewal of the others") {
      withTopic { topic =>
        createTopic(topic)
        publish(topic, "0", "1")
        val elapsed = run(
          firstPoll(shareConsumerSettings.withMaxPollRecords(2).withAutoRenewal(true), topic)(records =>
            records.traverse_(r => r.renew *> r.accept).timed.map(_._1)
          )
        )
        elapsed should be < lock / 3
      }
    }

    it("should accept the rest of a poll while one record outlives its lock") {
      withTopic { topic =>
        createTopic(topic)
        publish(topic, "0", "1", "2", "3")
        val settings = shareConsumerSettings.withMaxPollRecords(4).withAutoRenewal(true)
        val acceptedBefore = run(
          Ref
            .of[IO, Vector[String]](Vector.empty)
            .flatMap { accepted =>
              firstPoll(settings, topic)(
                _.parTraverseN(4) { r =>
                  if (r.record.value == "0")
                    IO.sleep(lock + 1.second) *> accepted.get.flatTap(_ => r.accept)
                  else
                    r.accept *> accepted.update(_ :+ r.record.value).as(Vector.empty[String])
                }
                  .map(_.toVector.flatten)
              )
            }
        )
        acceptedBefore should contain theSameElementsAs Vector("1", "2", "3")
        assertSettled(topic, settings)
      }
    }
  }

  describe("pausing") {
    it("should send an acknowledgement made while paused") {
      withTopic { topic =>
        createTopic(topic)
        publish(topic, "0")
        val settings = shareConsumerSettings.withoutMaxAcknowledgementDelay
        run(
          SignallingRef[IO, Boolean](false).flatMap { paused =>
            // The pause comes before the acknowledgement, and process outlives the record's lock, so
            // only the pause sends it.
            firstPoll(settings.withPauseWhen(paused), topic)(
              _.traverse_(r =>
                paused.set(true) *> IO.sleep(1.second) *> r.accept *> IO.sleep(lock + 1.second)
              )
            )
          }
        )
        assertSettled(topic, settings)
      }
    }

    it("should accept a poll while paused in implicit mode") {
      withTopic { topic =>
        createTopic(topic)
        publish(topic, "0")
        val settings = shareConsumerSettings.withProperty("share.acknowledgement.mode", "implicit")
        run(
          SignallingRef[IO, Boolean](false).flatMap { paused =>
            KafkaShareConsumer
              .resource(settings.withPauseWhen(paused))
              .use { consumer =>
                consumer.subscribeTo(topic) *>
                  consumer
                    .autoConsume(_ => paused.set(true))
                    .background
                    // Past the record's lock, also after joining the group.
                    .surround(IO.sleep(lock + 5.seconds)) *> consumer.stopConsuming
              }
          }
        )
        assertSettled(topic, settings.withProperty("share.acknowledgement.mode", "explicit"))
      }
    }
  }

  describe("stopping") {
    it("should send the last acknowledgements before finishing after stopping") {
      withTopic { topic =>
        createTopic(topic)
        publish(topic, "0")
        val settings = shareConsumerSettings
        run(
          consumed(settings, topic)(consumer => _.traverse_(r => consumer.stopConsuming *> r.accept))
        )
        assertSettled(topic, settings)
      }
    }

    it("should release records still processing when consume is canceled") {
      withTopic { topic =>
        createTopic(topic)
        publish(topic, "0")
        val settings = shareConsumerSettings
        run(
          Deferred[IO, Unit].flatMap { received =>
            consumed(settings, topic)(_ => _ => received.complete(()) *> IO.never[Unit])
              .race(received.get)
              .void
          }
        )
        readUntil(topic, settings, "0") shouldBe Vector("0" -> 2)
      }
    }
  }

  describe("several consumers") {
    it("should return the union of the consumers' subscriptions") {
      withTopic { topic =>
        createTopic(topic)
        val subscription = run(
          KafkaShareConsumers
            .resource(shareConsumerSettings, 2)
            .use(consumers => consumers.consumers.last.subscribeTo(topic) *> consumers.subscription)
        )
        subscription shouldBe SortedSet(topic)
      }
    }

    it("should settle every record once across consumers with suffixed client ids") {
      withTopic { topic =>
        createTopic(topic, partitions = 3)
        val produced = (0 until 30).map(_.toString)
        publish(topic, produced*)
        val (clientIds, consumed) = run(
          Ref
            .of[IO, Vector[String]](Vector.empty)
            .flatMap { seen =>
              KafkaShareConsumers
                .resource(shareConsumerSettings.withClientId("app").withMaxPollRecords(5), 3)
                .use { consumers =>
                  consumers.subscribeTo(topic) *>
                    consumers.consume(
                      _.parTraverseN(8) { r =>
                        r.accept *> seen
                          .updateAndGet(_ :+ r.record.value)
                          .flatMap(s => consumers.stopConsuming.whenA(s.size >= produced.size))
                      }.void
                    ) *>
                    seen.get
                      .map(
                        consumers.consumers.toVector.map(_.settings.properties("client.id")) -> _
                      )
                }
            }
        )
        clientIds should contain theSameElementsAs Vector("app-0", "app-1", "app-2")
        consumed should contain theSameElementsAs produced
      }
    }

    it("should fail for a non-positive number of consumers") {
      val result = run(KafkaShareConsumers.resource(shareConsumerSettings, 0).use_.attempt)
      result.swap.toOption.get shouldBe a[ConfigException]
    }
  }

}
