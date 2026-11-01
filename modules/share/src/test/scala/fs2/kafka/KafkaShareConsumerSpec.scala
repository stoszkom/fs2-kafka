/*
 * Copyright 2018 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka

import scala.collection.immutable.SortedSet
import scala.concurrent.duration.*

import cats.~>
import cats.effect.{Deferred, IO, Ref}
import cats.syntax.all.*

import org.apache.kafka.common.config.ConfigException
import org.apache.kafka.common.errors.InvalidRecordStateException

final class KafkaShareConsumerSpec extends ShareConsumerBaseKafkaSpec {

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

    it("should report the subscription, lock duration and metrics") {
      withTopic { topic =>
        createCustomTopic(topic)
        publish(topic, "0")
        val (subscription, lockDuration, metrics) = run(
          KafkaShareConsumer
            .resource(shareConsumerSettings)
            .use { consumer =>
              consumer.subscribeTo(topic) *>
                consumer.records.take(1).evalMap(_.accept).compile.drain *>
                (consumer.subscription, consumer.acquisitionLockTimeout, consumer.metrics).tupled
            }
        )
        subscription shouldBe SortedSet(topic)
        lockDuration shouldBe Some(lock)
        metrics should not be empty
      }
    }

    it("should keep streaming across unsubscribe and a new subscription") {
      withTopic { first =>
        withTopic { second =>
          createCustomTopic(first)
          createCustomTopic(second)
          publish(first, "first")
          publish(second, "second")
          val consumed = run(
            KafkaShareConsumer
              .resource(shareConsumerSettings)
              .use { consumer =>
                consumer.subscribeTo(first) *>
                  consumer
                    .records
                    .evalMap { r =>
                      r.accept *>
                        (consumer.unsubscribe *> consumer.subscribeTo(second))
                          .whenA(r.record.value == "first")
                          .as(r.record.value)
                    }
                    .take(2)
                    .compile
                    .toVector
              }
          )
          consumed shouldBe Vector("first", "second")
        }
      }
    }

    it("should end awaitTermination normally once the consumer is closed") {
      run(
        KafkaShareConsumer
          .resource(shareConsumerSettings)
          .use(IO.pure)
          .flatMap(_.awaitTermination.timeout(10.seconds))
      )
    }

    it("should end the record stream normally once the consumer is closed") {
      withTopic { topic =>
        createCustomTopic(topic)
        val result = run(
          KafkaShareConsumer
            .resource(shareConsumerSettings)
            .allocated
            .flatMap { case (consumer, close) =>
              consumer.subscribeTo(topic) *>
                consumer
                  .records
                  .compile
                  .drain
                  .start
                  .flatMap { stream =>
                    IO.sleep(2.seconds) *> close *> stream.joinWithNever.attempt.timeout(10.seconds)
                  }
            }
        )
        result shouldBe Right(())
      }
    }

    it("should fail with ConsumerShutdownException once closed after a failure") {
      withTopic { topic =>
        createCustomTopic(topic)
        publish(topic, "0")
        val settings = ShareConsumerSettings(
          Deserializer[IO, String],
          Deserializer.fail[IO, String](new RuntimeException("failed"))
        ).withProperties(shareConsumerSettings.properties)
        val (failure, afterClose) = run(
          KafkaShareConsumer
            .resource(settings)
            .allocated
            .flatMap { case (consumer, close) =>
              (consumer.subscribeTo(topic) *> consumer.records.compile.drain.attempt).flatMap(
                failure => close *> consumer.commitSync.attempt.tupleLeft(failure)
              )
            }
        )
        failure.swap.toOption.get.getMessage shouldBe "failed"
        afterClose.swap.toOption.get shouldBe a[ConsumerShutdownException]
      }
    }

    it("should fail the record stream when not subscribed") {
      val result =
        run(KafkaShareConsumer.stream(shareConsumerSettings).records.compile.drain.attempt)
      result.swap.toOption.get shouldBe a[NotSubscribedException]
    }

    it("should fail a second record stream") {
      withTopic { topic =>
        createCustomTopic(topic)
        publish(topic, "0")
        val result = run(
          KafkaShareConsumer
            .resource(shareConsumerSettings)
            .use { consumer =>
              consumer.subscribeTo(topic) *>
                consumer
                  .records
                  .take(1)
                  .evalMap(r => consumer.records.compile.drain.attempt <* r.accept)
                  .compile
                  .lastOrError
            }
        )
        result.swap.toOption.get shouldBe a[StreamAlreadyStartedException]
      }
    }
  }

  describe("KafkaShareConsumer#records") {
    it("should consume all records, across partitions") {
      withTopic { topic =>
        createCustomTopic(topic, partitions = 3)
        val produced = (0 until 9).map(_.toString)
        publish(topic, produced*)
        val consumed = run(
          KafkaShareConsumer
            .stream(shareConsumerSettings)
            .subscribeTo(topic)
            .records
            .parEvalMapUnordered(4)(r => r.accept.as(r.record.value))
            .take(produced.size.toLong)
            .compile
            .toVector
        )
        consumed should contain theSameElementsAs produced
      }
    }

    it("should not deliver accepted or rejected records again") {
      withTopic { topic =>
        createCustomTopic(topic)
        publish(topic, "0", "1")
        val settings = shareConsumerSettings
        run(
          KafkaShareConsumer
            .stream(settings)
            .subscribeTo(topic)
            .records
            .take(2)
            .evalMap(r =>
              if (r.record.value == "0") r.accept
              else r.reject
            )
            .compile
            .drain
        )
        publish(topic, "after")
        readUntil(topic, settings, "after") shouldBe Vector("after" -> 1)
      }
    }

    it("should deliver released records again, with a higher delivery count") {
      withTopic { topic =>
        createCustomTopic(topic)
        publish(topic, "0")
        val deliveries = run(
          KafkaShareConsumer
            .stream(shareConsumerSettings)
            .subscribeTo(topic)
            .records
            .take(2)
            .zipWithIndex
            .evalMap { case (r, index) =>
              (if (index == 0L) r.release
               else r.accept).as(
                r.offset.deliveryCount.toInt
              )
            }
            .compile
            .toVector
        )
        deliveries shouldBe Vector(1, 2)
      }
    }

    it("should let two consumers process records of the same partition concurrently") {
      withTopic { topic =>
        createCustomTopic(topic)
        publish(topic, "0", "1")
        val settings = shareConsumerSettings.withMaxPollRecords(1)
        val consumed = run(
          Deferred[IO, Unit].flatMap { bothStarted =>
            Ref
              .of[IO, Int](0)
              .flatMap { started =>
                def consume: IO[String] =
                  KafkaShareConsumer
                    .stream(settings)
                    .subscribeTo(topic)
                    .records
                    .take(1)
                    .evalMap { r =>
                      started
                        .updateAndGet(_ + 1)
                        .flatMap(n => bothStarted.complete(()).whenA(n == 2)) *>
                        bothStarted.get *> r.accept.as(r.record.value)
                    }
                    .compile
                    .lastOrError
                (consume, consume).parTupled
              }
          }
        )
        Set(consumed._1, consumed._2) shouldBe Set("0", "1")
      }
    }
  }

  describe("acknowledging records") {
    // The second record stays undecided, so no poll sends the first's decision; the delay does.
    it("should send staged decisions after the max acknowledgement delay, without a poll") {
      withTopic { topic =>
        createCustomTopic(topic)
        publish(topic, "0", "1")
        val settings = shareConsumerSettings.withMaxPollRecords(2)
        val results  = run(
          KafkaShareConsumer
            .resource(settings.withMaxAcknowledgementDelay(100.millis))
            .use { consumer =>
              consumer.subscribeTo(topic) *> consumer
                .records
                .zipWithIndex
                .evalMap {
                  case (r, 0) => r.accept.as(None)
                  case (r, _) => IO.pure(Some(r))
                }
                .unNone
                .take(1)
                .compile
                .lastOrError
                .flatMap(second =>
                  IO.sleep(1.second) *> consumer.commitSync <*
                    second.accept
                )
            }
        )
        results shouldBe empty
        publish(topic, "after")
        readUntil(topic, settings, "after") shouldBe Vector("after" -> 1)
      }
    }

    it("should commit staged decisions") {
      withTopic { topic =>
        createCustomTopic(topic)
        publish(topic, "0", "1")
        val settings = shareConsumerSettings
        val results  = run(
          KafkaShareConsumer
            .resource(settings)
            .use { consumer =>
              consumer.subscribeTo(topic) *> consumer
                .records
                .zipWithIndex
                .evalMap {
                  case (r, 0) => r.accept.as(None)
                  case (r, _) => IO.pure(Some(r))
                }
                .unNone
                .take(1)
                .compile
                .lastOrError
                .flatMap(second => consumer.commitSync <* second.accept)
            }
        )
        // Both records usually come in one poll, so the commit sends the first's decision; if not,
        // the poll for the second sent it.
        results.values.flatten shouldBe empty
        publish(topic, "after")
        readUntil(topic, settings, "after") shouldBe Vector("after" -> 1)
      }
    }
    // Without the delay or a poll, only commitAsync sends the first's decision before commitSync.
    it("should send staged decisions with commitAsync") {
      withTopic { topic =>
        createCustomTopic(topic)
        publish(topic, "0", "1")
        val settings = shareConsumerSettings.withMaxPollRecords(2)
        val results  = run(
          KafkaShareConsumer
            .resource(settings.withoutMaxAcknowledgementDelay)
            .use { consumer =>
              consumer.subscribeTo(topic) *> consumer
                .records
                .zipWithIndex
                .evalMap {
                  case (r, 0) => (r.accept *> consumer.commitAsync).as(None)
                  case (r, _) => IO.pure(Some(r))
                }
                .unNone
                .take(1)
                .compile
                .lastOrError
                .flatMap(second => IO.sleep(1.second) *> consumer.commitSync <* second.accept)
            }
        )
        results shouldBe empty
        publish(topic, "after")
        readUntil(topic, settings, "after") shouldBe Vector("after" -> 1)
      }
    }

    it("should send staged decisions with the next poll, and the last ones when closing") {
      withTopic { topic =>
        createCustomTopic(topic)
        publish(topic, (0 until 10).map(_.toString)*)
        val settings = shareConsumerSettings.withMaxPollRecords(5)
        run(
          KafkaShareConsumer
            .resource(settings)
            .use { consumer =>
              consumer.subscribeTo(topic) *> consumer
                .records
                .map(_.offset -> (AcknowledgeType.Accept: AcknowledgeType))
                .take(10)
                .through(acknowledgeChunks[IO])
                .compile
                .drain
            }
        )
        publish(topic, "after")
        readUntil(topic, settings, "after") shouldBe Vector("after" -> 1)
      }
    }
    it("should let the last of several decisions staged together for a record win") {
      withTopic { topic =>
        createCustomTopic(topic)
        publish(topic, "0")
        val settings = shareConsumerSettings
        run(
          KafkaShareConsumer
            .resource(settings)
            .use { consumer =>
              consumer.subscribeTo(topic) *> consumer
                .records
                .take(1)
                .flatMap(r =>
                  fs2.Stream(
                    r.offset -> (AcknowledgeType.Accept: AcknowledgeType),
                    r.offset -> (AcknowledgeType.Release: AcknowledgeType)
                  )
                )
                .through(acknowledgeChunks[IO])
                .compile
                .drain *> consumer.commitSync
            }
        )
        // The release won, so the record is delivered again (this consumer may poll it once more).
        val again = readUntil(topic, settings, "0")
        again.map(_._1) shouldBe Vector("0")
        again.head._2 should be >= 2
      }
    }
    it("should fail a decision for a delivery whose decision was sent") {
      withTopic { topic =>
        createCustomTopic(topic)
        publish(topic, "0")
        val late = run(
          KafkaShareConsumer
            .resource(shareConsumerSettings)
            .use { consumer =>
              consumer.subscribeTo(topic) *> consumer
                .records
                .take(1)
                .evalMap(r =>
                  r.accept *> consumer.commitSync *>
                    r.reject.attempt
                )
                .compile
                .lastOrError
            }
        )
        late.swap.toOption.get shouldBe a[RecordNotInFlightException]
      }
    }
    // A commit timeout far below a broker round trip, so the broker cannot confirm in time. The
    // second record stays undecided, so that no poll sends the first's decision before the commit.
    it("should fail a commit which the broker does not confirm in time") {
      withTopic { topic =>
        createCustomTopic(topic)
        publish(topic, "0", "1")
        val result = run(
          KafkaShareConsumer
            .resource(shareConsumerSettings.withMaxPollRecords(2))
            .use { consumer =>
              consumer.subscribeTo(topic) *> consumer
                .records
                .take(1)
                .evalMap(_.accept *> consumer.commitSync(1.nano).attempt)
                .compile
                .lastOrError
            }
        )
        result.swap.toOption.get shouldBe a[ShareCommitTimeoutException]
      }
    }

    it("should reject a non-positive commit timeout") {
      val result = run(
        KafkaShareConsumer.resource(shareConsumerSettings).use(_.commitSync(Duration.Zero).attempt)
      )
      result.swap.toOption.get shouldBe an[IllegalArgumentException]
    }

    it("should fail an acknowledgement through the offset of an earlier delivery") {
      withTopic { topic =>
        createCustomTopic(topic)
        publish(topic, "0")
        val stale = run(
          KafkaShareConsumer
            .stream(shareConsumerSettings)
            .subscribeTo(topic)
            .records
            .take(2)
            .evalMapAccumulate(Option.empty[AcknowledgeableOffset[IO]]) {
              case (None, r) =>
                r.release.as(Some(r.offset) -> None)
              case (Some(earlier), r) =>
                (earlier.accept.attempt <* r.accept).map(result => Some(earlier) -> Some(result))
            }
            .map(_._2)
            .unNone
            .compile
            .lastOrError
        )
        stale.swap.toOption.get shouldBe a[RecordNotInFlightException]
      }
    }

  }

  describe("acknowledgeChunks") {
    it("should stage offsets mapped with different functions through each") {
      withTopic { topic =>
        createCustomTopic(topic)
        publish(topic, "0", "1")
        val calls = run(
          Ref
            .of[IO, Set[String]](Set.empty)
            .flatMap { called =>
              def tagged(tag: String): IO ~> IO = new (IO ~> IO) {
                def apply[A](io: IO[A]): IO[A] = called.update(_ + tag) *> io
              }
              val (first, second) = (tagged("first"), tagged("second"))
              KafkaShareConsumer
                .resource(shareConsumerSettings)
                .use { consumer =>
                  consumer.subscribeTo(topic) *> consumer
                    .records
                    .take(2)
                    .map { r =>
                      val f = if (r.record.value == "0") first else second
                      r.offset.mapK(f) -> (AcknowledgeType.Accept: AcknowledgeType)
                    }
                    .through(acknowledgeChunks[IO])
                    .compile
                    .drain
                } *> called.get
            }
        )
        calls shouldBe Set("first", "second")
      }
    }

    it("should settle every record once") {
      withTopic { topic =>
        createCustomTopic(topic, partitions = 2)
        val produced = (0 until 30).map(_.toString)
        publish(topic, produced*)
        val consumed = run(
          KafkaShareConsumer
            .resource(shareConsumerSettings.withMaxPollRecords(5))
            .use { consumer =>
              Ref
                .of[IO, Vector[(String, Int)]](Vector.empty)
                .flatMap { seen =>
                  consumer.subscribeTo(topic) *>
                    consumer
                      .records
                      .parEvalMapUnordered(4) { r =>
                        seen
                          .updateAndGet(_ :+ (r.record.value -> r.offset.deliveryCount.toInt))
                          .flatTap(s => consumer.stopConsuming.whenA(s.size >= produced.size))
                          .as(r.offset -> (AcknowledgeType.Accept: AcknowledgeType))
                      }
                      .through(acknowledgeChunks[IO])
                      .compile
                      .drain *> seen.get
                }
            }
        )
        consumed.map(_._1) should contain theSameElementsAs produced
        all(consumed.map(_._2)) shouldBe 1
      }
    }

    it("should stage a chunk's decisions with each of its consumers") {
      withTopic { topic =>
        val other = topic + "-other"
        createCustomTopic(topic)
        createCustomTopic(other)
        publish(topic, "a")
        publish(other, "b")
        val settings = shareConsumerSettings
        run(
          (KafkaShareConsumer.resource(settings), KafkaShareConsumer.resource(settings))
            .tupled
            .use { case (first, second) =>
              first.subscribeTo(topic) *> second.subscribeTo(other) *>
                (first.records.take(1).compile.toList, second.records.take(1).compile.toList)
                  .parTupled
                  .flatMap { case (a, b) =>
                    fs2
                      .Stream
                      .emits((a ++ b).map(_.offset -> (AcknowledgeType.Accept: AcknowledgeType)))
                      .through(acknowledgeChunks[IO])
                      .compile
                      .drain
                  } *> first.commitSync *> second.commitSync
            }
        )
        publish(topic, "after")
        publish(other, "after")
        readUntil(topic, settings, "after") shouldBe Vector("after" -> 1)
        readUntil(other, settings, "after") shouldBe Vector("after" -> 1)
      }
    }
  }

  describe("implicit acknowledgement") {
    // The same share group in explicit acknowledgement mode, so records can be read back and
    // accepted.
    def explicitly(settings: ShareConsumerSettings[IO, String, String]) =
      settings.withProperty("share.acknowledgement.mode", "explicit")

    def implicitSettings: ShareConsumerSettings[IO, String, String] =
      shareConsumerSettings
        .withProperty("share.acknowledgement.mode", "implicit")
        .withMaxPollRecords(3)

    it("should accept the records of a poll once they have all been handed out") {
      withTopic { topic =>
        createCustomTopic(topic)
        val produced = (0 until 6).map(_.toString)
        publish(topic, produced*)
        val settings = implicitSettings
        val consumed = run(
          KafkaShareConsumer
            .resource(settings)
            .use { consumer =>
              consumer.subscribeTo(topic) *>
                consumer
                  .records
                  .map(_.record.value)
                  .evalTap(value => consumer.stopConsuming.whenA(value == produced.last))
                  .compile
                  .toVector
            }
        )
        consumed shouldBe produced
        publish(topic, "after")
        readUntil(topic, explicitly(settings), "after") shouldBe Vector("after" -> 1)
      }
    }

    it("should not accept a poll whose records were not all handed out when the stream ends") {
      withTopic { topic =>
        createCustomTopic(topic)
        publish(topic, "0", "1", "2")
        val settings = implicitSettings
        run(KafkaShareConsumer.stream(settings).subscribeTo(topic).records.take(1).compile.drain)
        readUntil(topic, explicitly(settings), "2").map(_._2) shouldBe Vector(2, 2, 2)
      }
    }

    it("should fail acknowledgements with an ImplicitAcknowledgementException") {
      withTopic { topic =>
        createCustomTopic(topic)
        publish(topic, "0")
        val result = run(
          KafkaShareConsumer
            .stream(implicitSettings)
            .subscribeTo(topic)
            .records
            .evalMap(_.accept.attempt)
            .take(1)
            .compile
            .lastOrError
        )
        result.swap.toOption.get shouldBe an[ImplicitAcknowledgementException]
      }
    }
    it("should not check the commit timeout against the lock in implicit mode") {
      withTopic { topic =>
        createCustomTopic(topic)
        publish(topic, "0")
        val settings = shareConsumerSettings
          .withAcknowledgementMode(ShareAcknowledgementMode.Implicit)
          .withAutoRenewal(true)
          .withCommitTimeout(lock)
        val values = run(
          KafkaShareConsumer
            .resource(settings)
            .use { consumer =>
              consumer.subscribeTo(topic) *> consumer
                .records
                .evalTap(_ => consumer.stopConsuming)
                .map(_.record.value)
                .compile
                .toList
            }
        )
        values shouldBe List("0")
      }
    }
  }

  describe("batch_optimized acquisition") {
    it("should settle every record once") {
      withTopic { topic =>
        createCustomTopic(topic)
        val produced = (0 until 20).map(_.toString)
        publish(topic, produced*)
        val consumed = run(
          KafkaShareConsumer
            .stream(
              shareConsumerSettings
                .withProperty("share.acquire.mode", "batch_optimized")
                .withMaxPollRecords(2)
            )
            .subscribeTo(topic)
            .records
            .evalMap(r => r.accept.as(r.record.value -> r.offset.deliveryCount.toInt))
            .take(produced.size.toLong)
            .compile
            .toVector
        )
        consumed.map(_._1) should contain theSameElementsAs produced
        all(consumed.map(_._2)) shouldBe 1
      }
    }
  }

  describe("renewal") {
    it("should stage a decision made right after a renewal once the record is returned") {
      withTopic { topic =>
        createCustomTopic(topic)
        publish(topic, "0")
        val settings = shareConsumerSettings
        run(
          KafkaShareConsumer
            .resource(settings)
            .use { consumer =>
              consumer.subscribeTo(topic) *> consumer
                .records
                .take(1)
                .evalMap(r =>
                  r.renew *>
                    r.accept
                )
                .compile
                .drain
            }
        )
        publish(topic, "after")
        readUntil(topic, settings, "after") shouldBe Vector("after" -> 1)
      }
    }
    it("should send a decision for a renewed record while another record is slow") {
      withTopic { topic =>
        createCustomTopic(topic)
        publish(topic, "0", "1")
        val settings = shareConsumerSettings
        run(
          KafkaShareConsumer
            .resource(settings)
            .use { consumer =>
              consumer.subscribeTo(topic) *> consumer
                .records
                .take(2)
                .parEvalMapUnordered(2) { r =>
                  if (r.record.value == "0")
                    r.renew *> IO.sleep(1.second) *>
                      r.accept
                  else IO.sleep(lock * 4 / 3) *> r.accept
                }
                .compile
                .drain *> consumer.commitSync
            }
        )
        publish(topic, "after")
        readUntil(topic, settings, "after") shouldBe Vector("after" -> 1)
      }
    }

    it("should finish stopping after an unsubscribe while a renewal is pending") {
      withTopic { topic =>
        createCustomTopic(topic)
        publish(topic, "0", "1")
        run(
          KafkaShareConsumer
            .resource(shareConsumerSettings.withMaxPollRecords(2))
            .use { consumer =>
              consumer.subscribeTo(topic) *> consumer
                .records
                .take(2)
                .parEvalMapUnordered(2) { r =>
                  if (r.record.value == "0") r.renew
                  else IO.sleep(1.second) *> consumer.unsubscribe *> r.accept.attempt.void
                }
                .compile
                .drain *> consumer.stopConsuming *> consumer.awaitTermination.timeout(30.seconds)
            }
        )
      }
    }

    it("should release a record held past the max processing time") {
      withTopic { topic =>
        createCustomTopic(topic)
        publish(topic, "0")
        val settings = shareConsumerSettings
        val late     = run(
          KafkaShareConsumer
            .resource(settings.withMaxProcessingTime(3.seconds))
            .use { consumer =>
              consumer.subscribeTo(topic) *> consumer
                .records
                .take(1)
                .evalMap(r => IO.sleep(5.seconds) *> r.accept.attempt)
                .compile
                .lastOrError
            }
        )
        late.swap.toOption.get shouldBe a[RecordNotInFlightException]
        readUntil(topic, settings, "0").map(_._1) shouldBe Vector("0")
      }
    }
    it("should count the max processing time from the user's latest renewal") {
      withTopic { topic =>
        createCustomTopic(topic)
        publish(topic, "0")
        val settings = shareConsumerSettings
        val result   = run(
          KafkaShareConsumer
            .resource(settings.withMaxProcessingTime(3.seconds))
            .use { consumer =>
              consumer.subscribeTo(topic) *> consumer
                .records
                .take(1)
                .evalMap(r =>
                  IO.sleep(2.seconds) *> r.renew *>
                    IO.sleep(2.seconds) *> r.accept.attempt
                )
                .compile
                .lastOrError <* consumer.commitSync
            }
        )
        result shouldBe Right(())
        publish(topic, "after")
        readUntil(topic, settings, "after") shouldBe Vector("after" -> 1)
      }
    }
    it("should renew a record automatically while it is processed past its lock") {
      withTopic { topic =>
        createCustomTopic(topic)
        publish(topic, "0")
        val settings = shareConsumerSettings.withAutoRenewal(true)
        val count    = run(
          KafkaShareConsumer
            .stream(settings)
            .subscribeTo(topic)
            .records
            .take(1)
            .evalMap(r => IO.sleep(lock + 1.second) *> r.accept.as(r.offset.deliveryCount.toInt))
            .compile
            .lastOrError
        )
        count shouldBe 1
        publish(topic, "after")
        readUntil(topic, settings, "after") shouldBe Vector("after" -> 1)
      }
    }

    it("should keep renewing records processed downstream after the stream ended") {
      withTopic { topic =>
        createCustomTopic(topic)
        publish(topic, "0", "1")
        val settings = shareConsumerSettings.withAutoRenewal(true)
        val counts   = run(
          KafkaShareConsumer
            .resource(settings)
            .use { consumer =>
              consumer.subscribeTo(topic) *> consumer
                .records
                .take(2)
                .parEvalMapUnordered(2)(r =>
                  IO.sleep(lock + 1.second) *> r.accept.as(r.offset.deliveryCount.toInt)
                )
                .compile
                .toVector
            }
        )
        counts shouldBe Vector(1, 1)
        publish(topic, "after")
        readUntil(topic, settings, "after") shouldBe Vector("after" -> 1)
      }
    }

    it("should keep a record past its lock with manual renewals only") {
      withTopic { topic =>
        createCustomTopic(topic)
        publish(topic, "0")
        val settings = shareConsumerSettings
        run(
          KafkaShareConsumer
            .stream(settings.withAutoRenewal(false))
            .subscribeTo(topic)
            .records
            .take(1)
            .evalMap(r =>
              (IO.sleep(lock / 3) *> r.renew).replicateA_(
                4
              ) *> r.accept
            )
            .compile
            .drain
        )
        publish(topic, "after")
        readUntil(topic, settings, "after") shouldBe Vector("after" -> 1)
      }
    }

    // The consumer does not track lock expiry itself, so the broker reports the error. The second
    // record stays undecided, so that the commit sends the first's decision.
    it("should lose a record past its lock without renewal, with the broker's error") {
      withTopic { topic =>
        createCustomTopic(topic)
        publish(topic, "0", "1")
        val settings = shareConsumerSettings.withMaxPollRecords(2)
        val results  = run(
          KafkaShareConsumer
            .resource(settings.withAutoRenewal(false))
            .use { consumer =>
              consumer.subscribeTo(topic) *> consumer
                .records
                .take(1)
                .evalMap(r =>
                  IO.sleep(lock + 1.second) *> r.accept *>
                    consumer.commitSync
                )
                .compile
                .lastOrError
            }
        )
        val error = results.values.toList.flatten.head
        error shouldBe an[InvalidRecordStateException]
        error should not be a[RecordNotInFlightException]
        all(readUntil(topic, settings, "1").map(_._2)) should be >= 2
      }
    }

    it("should accept a renewed record without waiting for the next renewal of the others") {
      withTopic { topic =>
        createCustomTopic(topic)
        publish(topic, "0", "1")
        val elapsed = run(
          Deferred[IO, FiniteDuration].flatMap { first =>
            KafkaShareConsumer
              .stream(shareConsumerSettings.withMaxPollRecords(2).withAutoRenewal(true))
              .subscribeTo(topic)
              .records
              .evalTap(_ => IO.monotonic.flatMap(first.complete))
              .evalMap(r => r.renew *> r.accept)
              .take(2)
              .compile
              .drain *> (IO.monotonic, first.get).mapN(_ - _)
          }
        )
        elapsed should be < lock / 3
      }
    }

    it("should accept the rest of a poll while one record outlives its lock") {
      withTopic { topic =>
        createCustomTopic(topic)
        publish(topic, "0", "1", "2", "3")
        val settings       = shareConsumerSettings.withMaxPollRecords(4).withAutoRenewal(true)
        val acceptedBefore = run(
          Ref
            .of[IO, Vector[String]](Vector.empty)
            .flatMap { accepted =>
              KafkaShareConsumer
                .stream(settings)
                .subscribeTo(topic)
                .records
                .parEvalMapUnordered(4) { r =>
                  if (r.record.value == "0")
                    IO.sleep(lock + 1.second) *> accepted.get.flatTap(_ => r.accept)
                  else
                    r.accept *> accepted.update(_ :+ r.record.value).as(Vector.empty[String])
                }
                .take(4)
                .compile
                .toVector
                .map(_.flatten)
            }
        )
        acceptedBefore should contain theSameElementsAs Vector("1", "2", "3")
        publish(topic, "after")
        readUntil(topic, settings, "after") shouldBe Vector("after" -> 1)
      }
    }
  }

  describe("stopping") {
    it("should release the records of a poll the stream never took") {
      withTopic { topic =>
        createCustomTopic(topic)
        publish(topic, (0 until 4).map(_.toString)*)
        // The stream ends after the first poll, so a second poll, if any, is never taken.
        val settings          = shareConsumerSettings.withMaxPollRecords(2)
        val (elapsed, others) = run(
          KafkaShareConsumer
            .resource(settings)
            .use { consumer =>
              consumer.subscribeTo(topic) *> consumer
                .records
                .take(2)
                .evalMap(_.accept)
                .compile
                .drain *> IO.delay(readUntil(topic, settings, "3")).timed
            }
        )
        elapsed should be < lock
        others.map(_._1).toSet shouldBe Set("2", "3")
      }
    }
    it("should send the last decisions before finishing after stopping") {
      withTopic { topic =>
        createCustomTopic(topic)
        publish(topic, "0")
        val settings = shareConsumerSettings
        val results  = run(
          KafkaShareConsumer
            .resource(settings)
            .use { consumer =>
              consumer.subscribeTo(topic) *> consumer
                .records
                .evalMap(r => consumer.stopConsuming *> r.accept)
                .compile
                .drain *> consumer.commitSync
            }
        )
        results.values.flatten shouldBe empty
        publish(topic, "after")
        readUntil(topic, settings, "after") shouldBe Vector("after" -> 1)
      }
    }
    it("should drain the records already polled on stopConsuming") {
      withTopic { topic =>
        createCustomTopic(topic)
        publish(topic, "0", "1", "2")
        val settings = shareConsumerSettings.withMaxPollRecords(3)
        val first    = run(
          KafkaShareConsumer
            .resource(settings)
            .use { consumer =>
              consumer.subscribeTo(topic) *>
                consumer
                  .records
                  .evalMap(r => consumer.stopConsuming *> r.accept.as(r.record.value))
                  .compile
                  .toVector <* consumer.awaitTermination
            }
        )
        first should not be empty
        publish(topic, "after")
        (first ++ readUntil(topic, settings, "after").map(_._1)) should contain theSameElementsAs
          Vector("0", "1", "2", "after")
      }
    }

    it("should release undecided records on stopConsuming after the stream ended") {
      withTopic { topic =>
        createCustomTopic(topic)
        publish(topic, "0", "1")
        val settings = shareConsumerSettings.withMaxPollRecords(2)
        run(
          KafkaShareConsumer
            .resource(settings)
            .use { consumer =>
              consumer.subscribeTo(topic) *>
                consumer.records.take(1).evalMap(_.accept).compile.drain *>
                consumer.stopConsuming *> consumer.awaitTermination
            }
        )
        readUntil(topic, settings, "1").last shouldBe ("1" -> 2)
      }
    }

    it("should release records never handed out when the stream ends") {
      withTopic { topic =>
        createCustomTopic(topic)
        publish(topic, "0", "1", "2")
        val settings = shareConsumerSettings.withMaxPollRecords(3)
        run(
          KafkaShareConsumer
            .stream(settings)
            .subscribeTo(topic)
            .records
            .take(1)
            .evalMap(_.accept)
            .compile
            .drain
        )
        val start = System.nanoTime
        val rest  = readUntil(topic, settings, "2")
        (System.nanoTime - start).nanos should be < lock / 2
        rest shouldBe Vector("1" -> 2, "2" -> 2)
      }
    }

    it("should release records still processing when the consumer is closed") {
      withTopic { topic =>
        createCustomTopic(topic)
        publish(topic, "0")
        val settings = shareConsumerSettings
        run(
          Deferred[IO, Unit].flatMap { received =>
            KafkaShareConsumer
              .stream(settings)
              .subscribeTo(topic)
              .records
              .evalMap(_ => received.complete(()) *> IO.never[Unit])
              .interruptWhen(received.get.attempt)
              .compile
              .drain
          }
        )
        readUntil(topic, settings, "0") shouldBe Vector("0" -> 2)
      }
    }
  }

  describe("several consumers") {
    it("should settle every record once across merged records, with suffixed client ids") {
      withTopic { topic =>
        createCustomTopic(topic, partitions = 3)
        val produced = (0 until 30).map(_.toString)
        publish(topic, produced*)
        val (clientIds, consumed) = run(
          KafkaShareConsumer
            .resource(shareConsumerSettings.withClientId("app").withMaxPollRecords(5), 3)
            .use { consumers =>
              consumers.subscribeTo(topic) *>
                consumers
                  .records
                  .parEvalMapUnordered(8)(r => r.accept.as(r.record.value))
                  .take(produced.size.toLong)
                  .compile
                  .toVector
                  .map(consumers.consumers.toVector.map(_.settings.properties("client.id")) -> _)
            }
        )
        clientIds should contain theSameElementsAs Vector("app-0", "app-1", "app-2")
        consumed should contain theSameElementsAs produced
      }
    }

    it("should settle every record once with per-consumer records") {
      withTopic { topic =>
        createCustomTopic(topic, partitions = 2)
        val produced = (0 until 20).map(_.toString)
        publish(topic, produced*)
        val consumed = run(
          KafkaShareConsumer
            .resource(shareConsumerSettings.withMaxPollRecords(4), 2)
            .use { consumers =>
              consumers.subscribeTo(topic) *>
                consumers
                  .perConsumerRecords
                  .map(
                    _.parEvalMapUnordered(4)(r => r.accept.as(r.record.value))
                  )
                  .parJoinUnbounded
                  .take(produced.size.toLong)
                  .compile
                  .toVector
            }
        )
        consumed should contain theSameElementsAs produced
      }
    }

    it("should fail for a non-positive number of consumers") {
      val result = run(KafkaShareConsumer.resource(shareConsumerSettings, 0).use_.attempt)
      result.swap.toOption.get shouldBe a[ConfigException]
    }
  }

}
