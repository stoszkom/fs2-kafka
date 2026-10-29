/*
 * Copyright 2018 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka

import scala.collection.immutable.SortedSet
import scala.concurrent.duration.*

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
                consumer
                  .records
                  .take(1)
                  .evalMap(_.offset.acknowledge(AcknowledgeType.Accept).commit)
                  .compile
                  .drain *>
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
                      r.offset.acknowledge(AcknowledgeType.Accept).commit *>
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
                  .evalMap(r =>
                    consumer.records.compile.drain.attempt <* r
                      .offset
                      .acknowledge(AcknowledgeType.Accept)
                      .commit
                  )
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
            .parEvalMapUnordered(4)(r =>
              r.offset.acknowledge(AcknowledgeType.Accept).commit.as(r.record.value)
            )
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
              if (r.record.value == "0") r.offset.acknowledge(AcknowledgeType.Accept).commit
              else r.offset.acknowledge(AcknowledgeType.Reject).commit
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
              (if (index == 0L) r.offset.acknowledge(AcknowledgeType.Release).commit
               else r.offset.acknowledge(AcknowledgeType.Accept).commit).as(
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
                        bothStarted.get *> r
                          .offset
                          .acknowledge(AcknowledgeType.Accept)
                          .commit
                          .as(r.record.value)
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
    // A commit timeout far below a broker round trip, so the broker cannot confirm in time.
    it("should fail a commit which the broker does not confirm in time") {
      withTopic { topic =>
        createCustomTopic(topic)
        publish(topic, "0")
        val result = run(
          KafkaShareConsumer
            .stream(shareConsumerSettings.withCommitTimeout(1.nano))
            .subscribeTo(topic)
            .records
            .take(1)
            .evalMap(_.offset.acknowledge(AcknowledgeType.Accept).commit.attempt)
            .compile
            .lastOrError
        )
        result.swap.toOption.get shouldBe a[ShareCommitTimeoutException]
      }
    }

    it("should fail a second acknowledgement of a delivery") {
      withTopic { topic =>
        createCustomTopic(topic)
        publish(topic, "0")
        val (again, renew) = run(
          KafkaShareConsumer
            .stream(shareConsumerSettings)
            .subscribeTo(topic)
            .records
            .take(1)
            .evalMap(r =>
              r.offset.acknowledge(AcknowledgeType.Accept).commit *> (
                r.offset.acknowledge(AcknowledgeType.Accept).commit.attempt,
                r.offset.acknowledge(AcknowledgeType.Renew).commit.attempt
              ).tupled
            )
            .compile
            .lastOrError
        )
        again.swap.toOption.get shouldBe a[RecordNotInFlightException]
        renew.swap.toOption.get shouldBe a[RecordNotInFlightException]
      }
    }

    it("should report a second acknowledgement of a delivery as a result of its partition") {
      withTopic { topic =>
        createCustomTopic(topic)
        publish(topic, "0")
        val (first, again) = run(
          KafkaShareConsumer
            .stream(shareConsumerSettings)
            .subscribeTo(topic)
            .records
            .take(1)
            .evalMap { r =>
              val accept = AcknowledgedOffsetBatch
                .fromFoldable(List(r.offset.acknowledge(AcknowledgeType.Accept)))
                .commitWithResults
              (accept, accept).tupled
            }
            .compile
            .lastOrError
        )
        first.values.toList shouldBe List(None)
        again.values.toList.flatten.head shouldBe a[RecordNotInFlightException]
      }
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
                r.offset.acknowledge(AcknowledgeType.Release).commit.as(Some(r.offset) -> None)
              case (Some(earlier), r) =>
                (earlier.acknowledge(AcknowledgeType.Accept).commit.attempt <* r
                  .offset
                  .acknowledge(AcknowledgeType.Accept)
                  .commit).map(result => Some(earlier) -> Some(result))
            }
            .map(_._2)
            .unNone
            .compile
            .lastOrError
        )
        stale.swap.toOption.get shouldBe a[RecordNotInFlightException]
      }
    }

    it("should commit offsets by topic and partition") {
      withTopic { topic =>
        createCustomTopic(topic)
        publish(topic, "0")
        val settings = shareConsumerSettings
        val unknown  = run(
          KafkaShareConsumer
            .resource(settings)
            .use { consumer =>
              consumer.subscribeTo(topic) *>
                consumer
                  .records
                  .take(1)
                  .evalMap { r =>
                    val partition = r.offset.topicPartition
                    consumer
                      .commitSync(
                        Map(partition -> Map(r.offset.offset + 1 -> AcknowledgeType.Accept))
                      )
                      .attempt <*
                      consumer.commitAsync(
                        Map(partition -> Map(r.offset.offset -> AcknowledgeType.Accept))
                      )
                  }
                  .compile
                  .lastOrError
            }
        )
        unknown.swap.toOption.get shouldBe a[RecordNotInFlightException]
        publish(topic, "after")
        readUntil(topic, settings, "after") shouldBe Vector("after" -> 1)
      }
    }

    it("should commit a batch, including a renewal and an acceptance of one record") {
      withTopic { topic =>
        createCustomTopic(topic)
        publish(topic, "0", "1", "2")
        val settings = shareConsumerSettings.withMaxPollRecords(3)
        run(
          KafkaShareConsumer
            .resource(settings)
            .use { consumer =>
              consumer.subscribeTo(topic) *>
                consumer
                  .records
                  .take(3)
                  .flatMap(r =>
                    fs2
                      .Stream(AcknowledgeType.Renew, AcknowledgeType.Accept)
                      .map(r.offset.acknowledge)
                  )
                  .compile
                  .toVector
                  .flatMap(AcknowledgedOffsetBatch.fromFoldable(_).commit)
            }
        )
        publish(topic, "after")
        readUntil(topic, settings, "after") shouldBe Vector("after" -> 1)
      }
    }
  }

  describe("commitPollWithin") {
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
                          .update(_ :+ (r.record.value -> r.offset.deliveryCount.toInt))
                          .as(r.offset.acknowledge(AcknowledgeType.Accept))
                      }
                      .through(commitPollWithin(1.second))
                      .evalMap(_ =>
                        seen.get.flatMap(s => consumer.stopConsuming.whenA(s.size >= produced.size))
                      )
                      .compile
                      .drain *> seen.get
                }
            }
        )
        consumed.map(_._1) should contain theSameElementsAs produced
        all(consumed.map(_._2)) shouldBe 1
      }
    }

    it("should commit as soon as a poll is acknowledged, without waiting for maxDelay") {
      withTopic { topic =>
        createCustomTopic(topic)
        publish(topic, (0 until 6).map(_.toString)*)
        val elapsed = run(
          Deferred[IO, FiniteDuration].flatMap { first =>
            KafkaShareConsumer
              .stream(shareConsumerSettings.withMaxPollRecords(3))
              .subscribeTo(topic)
              .records
              .evalTap(_ => IO.monotonic.flatMap(first.complete))
              .map(_.offset.acknowledge(AcknowledgeType.Accept))
              .through(commitPollWithin(7.seconds))
              .take(2)
              .compile
              .drain *> (IO.monotonic, first.get).mapN(_ - _)
          }
        )
        elapsed should be < 3.seconds
      }
    }

    // The unacknowledged record holds back the poll, so the other record's offset waits for
    // maxDelay, past the renewal interval. It is renewed meanwhile, and committed once.
    it("should commit an offset which waited past the renewal interval") {
      withTopic { topic =>
        createCustomTopic(topic)
        publish(topic, "0", "1")
        val settings = shareConsumerSettings.withMaxPollRecords(2).withAutoRenewal(true)
        run(
          KafkaShareConsumer
            .stream(settings)
            .subscribeTo(topic)
            .records
            .filter(_.record.value == "0")
            .map(_.offset.acknowledge(AcknowledgeType.Accept))
            .through(commitPollWithin(lock))
            .take(1)
            .compile
            .drain
        )
        readUntil(topic, settings, "1") shouldBe Vector("1" -> 2)
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
            .evalMap(_.offset.acknowledge(AcknowledgeType.Accept).commit.attempt)
            .take(1)
            .compile
            .lastOrError
        )
        result.swap.toOption.get shouldBe an[ImplicitAcknowledgementException]
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
            .evalMap(r =>
              r.offset
                .acknowledge(AcknowledgeType.Accept)
                .commit
                .as(r.record.value -> r.offset.deliveryCount.toInt)
            )
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
            .evalMap(r =>
              IO.sleep(lock + 1.second) *> r
                .offset
                .acknowledge(AcknowledgeType.Accept)
                .commit
                .as(r.offset.deliveryCount.toInt)
            )
            .compile
            .lastOrError
        )
        count shouldBe 1
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
              (IO.sleep(lock / 3) *> r.offset.acknowledge(AcknowledgeType.Renew).commit)
                .replicateA_(4) *> r.offset.acknowledge(AcknowledgeType.Accept).commit
            )
            .compile
            .drain
        )
        publish(topic, "after")
        readUntil(topic, settings, "after") shouldBe Vector("after" -> 1)
      }
    }

    it("should lose a record past its lock without renewal") {
      withTopic { topic =>
        createCustomTopic(topic)
        publish(topic, "0")
        val settings = shareConsumerSettings
        val late     = run(
          KafkaShareConsumer
            .stream(settings.withAutoRenewal(false))
            .subscribeTo(topic)
            .records
            .evalMap(r =>
              IO.sleep(lock + 1.second) *> r
                .offset
                .acknowledge(AcknowledgeType.Accept)
                .commit
                .attempt
            )
            .take(1)
            .compile
            .lastOrError
        )
        late.swap.toOption.get shouldBe an[InvalidRecordStateException]
        all(readUntil(topic, settings, "0").map(_._2)) should be >= 2
      }
    }

    // The consumer does not track lock expiry itself, so the broker reports the error.
    it("should report the broker's error for a record whose lock expired") {
      withTopic { topic =>
        createCustomTopic(topic)
        publish(topic, "0")
        val results = run(
          KafkaShareConsumer
            .stream(shareConsumerSettings.withAutoRenewal(false))
            .subscribeTo(topic)
            .records
            .take(1)
            .evalMap(r =>
              IO.sleep(lock + 1.second) *>
                AcknowledgedOffsetBatch
                  .fromFoldable(List(r.offset.acknowledge(AcknowledgeType.Accept)))
                  .commitWithResults
            )
            .compile
            .lastOrError
        )
        val error = results.values.toList.flatten.head
        error shouldBe an[InvalidRecordStateException]
        error should not be a[RecordNotInFlightException]
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
              .evalMap(r =>
                r.offset.acknowledge(AcknowledgeType.Renew).commit *> r
                  .offset
                  .acknowledge(AcknowledgeType.Accept)
                  .commit
              )
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
                    IO.sleep(lock + 1.second) *> accepted
                      .get
                      .flatTap(_ => r.offset.acknowledge(AcknowledgeType.Accept).commit)
                  else
                    r.offset.acknowledge(AcknowledgeType.Accept).commit *> accepted
                      .update(_ :+ r.record.value)
                      .as(Vector.empty[String])
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
                  .evalMap(r =>
                    consumer.stopConsuming *> r
                      .offset
                      .acknowledge(AcknowledgeType.Accept)
                      .commit
                      .as(r.record.value)
                  )
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
            .evalMap(_.offset.acknowledge(AcknowledgeType.Accept).commit)
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
                  .parEvalMapUnordered(8)(r =>
                    r.offset.acknowledge(AcknowledgeType.Accept).commit.as(r.record.value)
                  )
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
                    _.parEvalMapUnordered(4)(r =>
                      r.offset.acknowledge(AcknowledgeType.Accept).commit.as(r.record.value)
                    )
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
