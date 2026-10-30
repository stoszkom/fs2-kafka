/*
 * Copyright 2018 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka

import java.time.Duration
import java.util.concurrent.atomic.{AtomicBoolean, AtomicReference}
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.Optional

import scala.collection.mutable
import scala.concurrent.duration.*

import cats.effect.unsafe.implicits.global
import cats.effect.IO
import fs2.kafka.consumer.MkShareConsumer
import fs2.kafka.internal.converters.collection.*

import org.apache.kafka.clients.consumer.{
  AcknowledgeType as KafkaAcknowledgeType,
  AcknowledgementCommitCallback,
  MockShareConsumer
}
import org.apache.kafka.clients.consumer.internals.{
  Acknowledgements,
  ShareFetch,
  ShareInFlightBatch
}
import org.apache.kafka.common.{KafkaException, TopicIdPartition, TopicPartition, Uuid}
import org.apache.kafka.common.record.TimestampType

/**
  * Behaviour when the broker does not confirm acknowledgements within the commit timeout, which a
  * test broker does not reproduce.
  */
final class KafkaShareConsumerTimeoutSpec extends BaseAsyncSpec {

  /**
    * A share consumer on Kafka's own in-flight tracking. It confirms acknowledgements during later
    * calls, like Java's consumer, but only while `confirming` is set; `commitSync` then times out.
    * Renewals submitted while `renewalError` is set fail. With `implicitMode`, polls and commits
    * accept the records of the previous poll, as in Java's implicit mode.
    */
  final private class TestShareConsumer(batches: List[Long]*)
      extends MockShareConsumer[Array[Byte], Array[Byte]] {

    val confirming                                      = new AtomicBoolean(true)
    val renewalError                                    = new AtomicReference[KafkaException]
    val acknowledged                                    = new ConcurrentLinkedQueue[(Long, KafkaAcknowledgeType)]
    var lock                                            = Optional.empty[Integer]()
    var implicitMode                                    = false
    private val tip                                     = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("topic", 0))
    private val next                                    = mutable.Queue(batches*)
    private val done                                    = mutable.Queue.empty[(Acknowledgements, KafkaException)]
    private val count                                   = mutable.Map.empty[Long, Int]
    private var fetch                                   = ShareFetch.empty[Array[Byte], Array[Byte]]()
    private var callback: AcknowledgementCommitCallback = _

    override def setAcknowledgementCommitCallback(value: AcknowledgementCommitCallback): Unit =
      callback = value

    override def acquisitionLockTimeoutMs(): Optional[Integer] = lock

    override def acknowledge(record: KafkaByteConsumerRecord, kind: KafkaAcknowledgeType): Unit =
      fetch.acknowledge(record, kind)

    override def commitAsync(): Unit = {
      confirm()
      submit()
    }

    override def commitSync(
      timeout: Duration
    ): java.util.Map[TopicIdPartition, Optional[KafkaException]] = {
      confirm()
      submit()
      if (!confirming.get)
        throw new org.apache.kafka.common.errors.TimeoutException("commitSync timed out")
      confirm()
      java.util.Collections.emptyMap()
    }

    private def submit(): Unit = {
      if (implicitMode) fetch.acknowledgeAll(KafkaAcknowledgeType.ACCEPT)
      fetch
        .takeAcknowledgedRecords()
        .asScala
        .values
        .foreach { node =>
          val acks  = node.acknowledgements()
          val types = acks.getAcknowledgementsTypeMap.asScala
          types.foreach { case (offset, kind) => acknowledged.add(offset.longValue -> kind) }
          val error =
            if (types.values.exists(_ == KafkaAcknowledgeType.RENEW)) renewalError.get else null
          acks.complete(error)
          done.enqueue(acks -> error)
        }
    }

    override def poll(timeout: Duration): KafkaByteConsumerRecords = {
      confirm()
      if (implicitMode) submit()
      fetch.takeRenewedRecords()
      if (fetch.isEmpty && !fetch.hasRenewals && next.nonEmpty) {
        val batch = new ShareInFlightBatch[Array[Byte], Array[Byte]](0, tip, lock)
        next.dequeue().foreach(offset => batch.addRecord(record(offset)))
        fetch = ShareFetch.empty[Array[Byte], Array[Byte]]()
        fetch.add(tip, batch)
      }
      new KafkaByteConsumerRecords(fetch.records(), java.util.Collections.emptyMap())
    }

    private def confirm(): Unit =
      while (confirming.get && done.nonEmpty) {
        val (acks, error) = done.dequeue()
        fetch.renew(Map(tip -> acks).asJava, lock): Unit
        if (callback != null)
          callback.onComplete(Map(tip -> acks.getAcknowledgementsTypeMap.keySet).asJava, error)
      }

    private def record(offset: Long): KafkaByteConsumerRecord = {
      val delivery = count.getOrElse(offset, 0) + 1
      count.update(offset, delivery)
      new KafkaByteConsumerRecord(
        "topic",
        0,
        offset,
        0L,
        TimestampType.CREATE_TIME,
        0,
        0,
        Array.emptyByteArray,
        Array.emptyByteArray,
        new org.apache.kafka.common.header.internals.RecordHeaders(),
        Optional.empty[Integer](),
        Optional.of(java.lang.Short.valueOf(delivery.toShort))
      )
    }

  }

  private val settings = ShareConsumerSettings[IO, String, String]
    .withPollTimeout(10.millis)
    .withPollInterval(10.millis)
    .withCommitTimeout(200.millis)
    .withAutoRenewal(false)

  private def consume[A](
    native: TestShareConsumer,
    config: ShareConsumerSettings[IO, String, String] = settings
  )(f: KafkaShareConsumer[IO, String, String] => IO[A]): A = {
    val factory = new MkShareConsumer[IO] {
      override def apply[G[_]](
        settings: ShareConsumerSettings[G, ?, ?]
      ): IO[KafkaByteShareConsumer] =
        IO.pure(native)
    }
    KafkaShareConsumer
      .resource(config)(implicitly, factory)
      .use(consumer => consumer.subscribeTo("topic") *> f(consumer))
      .timeout(10.seconds)
      .unsafeRunSync()
  }

  private def withoutConfirmation[A](native: TestShareConsumer)(io: IO[A]): IO[A] =
    IO(native.confirming.set(false)) *> io <* IO(native.confirming.set(true))

  describe("KafkaShareConsumer in implicit acknowledgement mode") {
    val implicitSettings = settings.withProperty("share.acknowledgement.mode", "implicit")

    it("should emit a redelivery of an offset it still tracks") {
      val native = new TestShareConsumer(List(1L), List(1L))
      native.implicitMode = true
      consume(native, implicitSettings)(
        _.records.map(r => r.offset.offset -> r.offset.deliveryCount.toInt).take(2).compile.toList
      ) shouldBe List(1L -> 1, 1L -> 2)
    }

    it("should read the acknowledgement mode case-insensitively, as Java does") {
      val native = new TestShareConsumer(List(1L), List(1L))
      native.implicitMode = true
      consume(native, settings.withProperty("share.acknowledgement.mode", "IMPLICIT"))(
        _.records.map(_.offset.offset).take(2).compile.toList
      ) shouldBe List(1L, 1L)
    }

    it("should accept a poll handed out completely when the stream ends") {
      val native = new TestShareConsumer(List(1L, 2L), List(3L))
      native.implicitMode = true
      consume(native, implicitSettings)(_.records.take(2).compile.drain)
      (native.acknowledged.asScala.toList should contain).allOf(
        1L -> KafkaAcknowledgeType.ACCEPT,
        2L -> KafkaAcknowledgeType.ACCEPT
      )
    }
  }

  describe("KafkaShareConsumer with batch_optimized acquisition") {
    it("should renew records left in Java's buffer by the age of the poll which fetched them") {
      // Records 1 and 2 fill the first poll, so record 3, returned by the second poll, may
      // come from the same fetch. With a 1s lock, renewal is due 500ms after the first poll, not
      // 500ms after the second, which starts about 300ms later.
      val native = new TestShareConsumer(List(1L, 2L), List(3L))
      native.lock = Optional.of(Integer.valueOf(1000))
      val config = settings
        .withAutoRenewal(true)
        .withCommitTimeout(100.millis)
        .withProperty("share.acquire.mode", "batch_optimized")
        .withMaxPollRecords(2)
      def renewed(offset: Long): IO[Unit] =
        IO(native.acknowledged.contains(offset -> KafkaAcknowledgeType.RENEW)).flatMap(done =>
          if (done) IO.unit else IO.sleep(5.millis) *> renewed(offset)
        )
      val renewedAfter = consume(native, config)(
        _.records
          .evalMapAccumulate(scala.concurrent.duration.Duration.Zero) { (start, r) =>
            r.offset.offset match {
              case 1L =>
                IO.monotonic
                  .flatMap(now =>
                    r.offset
                      .acknowledge(AcknowledgeType.Accept)
                      .commit
                      .as(now -> scala.concurrent.duration.Duration.Zero)
                  )
              case 2L =>
                (IO.sleep(300.millis) *> r.offset.acknowledge(AcknowledgeType.Accept).commit).as(
                  start -> scala.concurrent.duration.Duration.Zero
                )
              case _ =>
                (renewed(3L) *> IO.monotonic).flatMap(now =>
                  r.offset.acknowledge(AcknowledgeType.Accept).commit.as(start -> (now - start))
                )
            }
          }
          .map(_._2)
          .take(3)
          .compile
          .lastOrError
      )
      renewedAfter should be < 700.millis
    }
  }

  describe("KafkaShareConsumer with several commit requests") {
    it("should commit a synchronous request before asynchronous requests made after it") {
      val native            = new TestShareConsumer(List(1L, 2L))
      val (release, accept) = consume(native) { consumer =>
        consumer
          .records
          .take(2)
          .compile
          .toList
          .flatMap { records =>
            val List(first, second) = records: @unchecked
            // The unconfirmed commit holds the actor, so the next two requests wait together.
            for {
              _       <- IO(native.confirming.set(false))
              _       <- second.offset.acknowledge(AcknowledgeType.Accept).commit.attempt.start
              _       <- IO.sleep(50.millis)
              release <- consumer
                           .commitSync(
                             Map(first.offset.topicPartition -> Map(1L -> AcknowledgeType.Release))
                           )
                           .attempt
                           .start
              _      <- IO.sleep(20.millis)
              accept <- first.offset.acknowledge(AcknowledgeType.Accept).commit.attempt.start
              _      <- IO.sleep(20.millis) *> IO(native.confirming.set(true))
              r      <- release.joinWithNever
              a      <- accept.joinWithNever
            } yield r -> a
          }
      }
      release shouldBe Right(())
      accept.swap.toOption.get shouldBe a[RecordNotInFlightException]
      native.acknowledged.asScala.toList should not contain (1L -> KafkaAcknowledgeType.ACCEPT)
    }

    it("should send only the last decision for a record in one request") {
      val native = new TestShareConsumer(List(1L))
      val result = consume(native)(
        _.records
          .take(1)
          .evalMap { r =>
            AcknowledgedOffsetBatch
              .fromFoldable(
                List(
                  r.offset.acknowledge(AcknowledgeType.Renew),
                  r.offset.acknowledge(AcknowledgeType.Release),
                  r.offset.acknowledge(AcknowledgeType.Accept)
                )
              )
              .commit
              .attempt
          }
          .compile
          .lastOrError
      )
      result shouldBe Right(())
      native.acknowledged.asScala.toList shouldBe List(1L -> KafkaAcknowledgeType.ACCEPT)
    }

    it("should send only the last decision for a record among requests waiting together") {
      val native            = new TestShareConsumer(List(1L, 2L))
      val (release, accept) = consume(native) { consumer =>
        consumer
          .records
          .take(2)
          .compile
          .toList
          .flatMap { records =>
            val List(first, second) = records: @unchecked
            // The unconfirmed commit holds the actor, so the next two requests wait together.
            for {
              _       <- IO(native.confirming.set(false))
              _       <- second.offset.acknowledge(AcknowledgeType.Accept).commit.attempt.start
              _       <- IO.sleep(50.millis)
              release <- first.offset.acknowledge(AcknowledgeType.Release).commit.attempt.start
              _       <- IO.sleep(20.millis)
              accept  <- first.offset.acknowledge(AcknowledgeType.Accept).commit.attempt.start
              _       <- IO.sleep(20.millis) *> IO(native.confirming.set(true))
              r       <- release.joinWithNever
              a       <- accept.joinWithNever
            } yield r -> a
          }
      }
      release shouldBe Right(())
      accept shouldBe Right(())
      native.acknowledged.asScala.toList.filter(_._1 == 1L) shouldBe
        List(1L -> KafkaAcknowledgeType.ACCEPT)
    }
  }

  describe("KafkaShareConsumer without automatic renewal") {
    it("should renew the rest of the batch so that a renewed record can be accepted") {
      val native  = new TestShareConsumer(List(0L, 1L))
      val results = consume(native)(
        _.records
          .take(2)
          .evalMap { r =>
            val accept = r.offset.acknowledge(AcknowledgeType.Accept).commit.attempt
            if (r.offset.offset == 0L)
              r.offset.acknowledge(AcknowledgeType.Renew).commit *> accept
            else accept
          }
          .compile
          .toList
      )
      results shouldBe List(Right(()), Right(()))
      native.acknowledged.asScala.toList shouldBe List(
        0L -> KafkaAcknowledgeType.RENEW,
        1L -> KafkaAcknowledgeType.RENEW,
        0L -> KafkaAcknowledgeType.ACCEPT,
        1L -> KafkaAcknowledgeType.ACCEPT
      )
    }

    it(
      "should renew the rest of the batch so that a record whose renewal timed out can be accepted"
    ) {
      val native  = new TestShareConsumer(List(0L, 1L))
      val results = consume(native)(
        _.records
          .take(2)
          .evalMap { r =>
            val accept = r.offset.acknowledge(AcknowledgeType.Accept).commit.attempt
            if (r.offset.offset == 0L)
              withoutConfirmation(native)(
                r.offset.acknowledge(AcknowledgeType.Renew).commit.attempt
              ) *> accept
            else accept
          }
          .compile
          .toList
      )
      results shouldBe List(Right(()), Right(()))
      native.acknowledged.asScala.toList should contain(1L -> KafkaAcknowledgeType.RENEW)
    }
  }

  describe("KafkaShareConsumer when the broker does not confirm in time") {
    it("should fail the acknowledgement with a ShareCommitTimeoutException") {
      val native = new TestShareConsumer(List(1L))
      val result = consume(native)(
        _.records
          .take(1)
          .evalMap(r =>
            withoutConfirmation(native)(
              r.offset.acknowledge(AcknowledgeType.Accept).commit.attempt
            )
          )
          .compile
          .lastOrError
      )
      result.swap.toOption.get shouldBe a[ShareCommitTimeoutException]
    }

    it("should keep consuming when an automatic renewal times out") {
      val native = new TestShareConsumer(List(1L))
      native.lock = Optional.of(Integer.valueOf(1000))
      consume(native, settings.withAutoRenewal(true).withCommitTimeout(100.millis))(
        _.records
          .evalMap(r =>
            withoutConfirmation(native)(IO.sleep(700.millis)) *> r
              .offset
              .acknowledge(AcknowledgeType.Accept)
              .commit
          )
          .take(1)
          .compile
          .drain
      )
      native.acknowledged.asScala.toList should contain(1L -> KafkaAcknowledgeType.RENEW)
      native.acknowledged.asScala.last shouldBe (1L -> KafkaAcknowledgeType.ACCEPT)
    }

    it("should keep consuming when an automatic renewal fails") {
      val native = new TestShareConsumer(List(1L), List(2L))
      native.lock = Optional.of(Integer.valueOf(1000))
      native.renewalError.set(new KafkaException("renewal failed"))
      val results = consume(native, settings.withAutoRenewal(true).withCommitTimeout(100.millis))(
        _.records
          .take(2)
          .evalMap(r =>
            IO.whenA(r.offset.offset == 1L)(IO.sleep(700.millis)) *>
              r.offset.acknowledge(AcknowledgeType.Accept).commit.attempt
          )
          .compile
          .toList
      )
      native.acknowledged.asScala.toList should contain(1L -> KafkaAcknowledgeType.RENEW)
      results.head.swap.toOption.get shouldBe a[RecordNotInFlightException]
      results(1) shouldBe Right(())
    }

    it("should keep a record whose renewal through commitSync timed out") {
      val native  = new TestShareConsumer(List(1L))
      val emitted = consume(native)(consumer =>
        consumer
          .records
          .evalMap { r =>
            val renew =
              Map(r.offset.topicPartition -> Map(r.offset.offset -> AcknowledgeType.Renew))
            withoutConfirmation(native)(consumer.commitSync(renew).attempt).flatMap { renewed =>
              IO(renewed.swap.toOption.get shouldBe a[ShareCommitTimeoutException])
            } *> r.offset.acknowledge(AcknowledgeType.Accept).commit.as(r.offset.offset)
          }
          .interruptAfter(1.second)
          .compile
          .toList
      )
      emitted shouldBe List(1L)
    }

    it("should keep a record whose renewal timed out, without emitting it again") {
      val native  = new TestShareConsumer(List(1L))
      val emitted = consume(native)(
        _.records
          .evalMap { r =>
            withoutConfirmation(native)(
              r.offset.acknowledge(AcknowledgeType.Renew).commit.attempt
            ).flatMap { renewed =>
              IO(renewed.swap.toOption.get shouldBe a[ShareCommitTimeoutException])
            } *> r.offset.acknowledge(AcknowledgeType.Accept).commit.as(r.offset.offset)
          }
          .interruptAfter(1.second)
          .compile
          .toList
      )
      emitted shouldBe List(1L)
      native.acknowledged.asScala.toList shouldBe
        List(1L -> KafkaAcknowledgeType.RENEW, 1L -> KafkaAcknowledgeType.ACCEPT)
    }

    it("should forget a record whose timed-out renewal failed, once a new record is polled") {
      val native = new TestShareConsumer(List(1L), List(2L))
      native.renewalError.set(new KafkaException("renewal failed"))
      val results = consume(native)(
        _.records
          .take(2)
          .evalMap { r =>
            if (r.offset.offset == 1L)
              withoutConfirmation(native)(
                r.offset.acknowledge(AcknowledgeType.Renew).commit.attempt
              ) *> r.offset.acknowledge(AcknowledgeType.Accept).commit.attempt
            else r.offset.acknowledge(AcknowledgeType.Accept).commit.attempt
          }
          .compile
          .toList
      )
      results.head.swap.toOption.get shouldBe a[RecordNotInFlightException]
      results(1) shouldBe Right(())
    }

    it("should stop once a timed-out renewal can no longer complete, without a reported lock") {
      val native = new TestShareConsumer(List(1L))
      native.renewalError.set(new KafkaException("renewal failed"))
      consume(native, settings.withProperty("default.api.timeout.ms", "300"))(consumer =>
        consumer
          .records
          .evalMap(r =>
            withoutConfirmation(native)(
              r.offset.acknowledge(AcknowledgeType.Renew).commit.attempt
            ) *> consumer.stopConsuming
          )
          .compile
          .drain
      )
    }

    it(
      "should not keep renewing other records while an acceptance waits for an unconfirmed renewal"
    ) {
      val native = new TestShareConsumer(List(1L, 2L))
      native.lock = Optional.of(Integer.valueOf(30000))
      consume(native, settings.withAutoRenewal(true))(
        _.records
          .take(2)
          .parEvalMapUnordered(2) { r =>
            if (r.offset.offset == 1L)
              withoutConfirmation(native)(
                r.offset.acknowledge(AcknowledgeType.Renew).commit.attempt
              ) *> r.offset.acknowledge(AcknowledgeType.Accept).commit
            else IO.sleep(500.millis) *> r.offset.acknowledge(AcknowledgeType.Accept).commit
          }
          .compile
          .drain
      )
      native.acknowledged.asScala.count(_ == (2L -> KafkaAcknowledgeType.RENEW)) should be <= 1
    }
  }

}
