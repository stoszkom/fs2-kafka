/*
 * Copyright 2018 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka

import java.time.Duration
import java.util.concurrent.{Callable, Executors}
import java.util.Properties

import scala.concurrent.duration.*

import cats.effect.IO
import fs2.kafka.internal.converters.collection.*
import fs2.Pipe

import org.apache.kafka.clients.consumer.{
  AcknowledgeType => JavaAcknowledgeType,
  ConsumerRecord => JavaConsumerRecord,
  KafkaShareConsumer => JavaShareConsumer
}
import org.apache.kafka.common.serialization.StringDeserializer

/**
  * Compares the throughput of fs2's share consumer with Java's own, against the test broker. It
  * runs only when the `SHARE_BENCHMARK` environment variable is set, for example:
  *
  * {{{
  * SHARE_BENCHMARK=1 sbt "share/testOnly fs2.kafka.ShareBenchmarkSpec"
  * }}}
  *
  * Each run reads the whole topic with a new share group and is timed from its first record, since
  * joining a share group takes several seconds.
  */
final class ShareBenchmarkSpec extends ShareConsumerBaseKafkaSpec {

  // Skipped as a whole unless requested, so no broker is started for it.
  override def run(testName: Option[String], args: org.scalatest.Args): org.scalatest.Status =
    if (sys.env.contains("SHARE_BENCHMARK")) super.run(testName, args)
    else org.scalatest.SucceededStatus

  private type Records = fs2.Stream[IO, AcknowledgeableConsumerRecord[IO, String, String]]

  private val parallelism = 32

  // When the first record arrived, and when the last acknowledgement was confirmed.
  @volatile private var firstAt = 0L
  @volatile private var lastAt  = 0L

  private def markFirst[A]: Pipe[IO, A, A] =
    _.mapChunks { chunk =>
      if (firstAt == 0L) firstAt = System.nanoTime
      chunk
    }

  // Takes records until `count` distinct ones arrived, so redeliveries count once, as for Java.
  private def distinct[A <: AcknowledgeableConsumerRecord[IO, String, String]](
    count: Int
  ): Pipe[IO, A, A] =
    records =>
      fs2
        .Stream
        .suspend {
          val seen = scala.collection.mutable.HashSet.empty[(String, Int, Long)]
          records.takeThrough { record =>
            seen += ((record.record.topic, record.record.partition, record.offset.offset))
            seen.size < count
          }
        }

  private def time(name: String, count: Int)(run: => Unit): Unit = {
    firstAt = 0L
    lastAt = 0L
    run
    val seconds = (lastAt - firstAt) / 1e9
    println(f"BENCHMARK $name%-48s ${count / seconds}%10.0f records/s")
  }

  // Sends the last pending acknowledgements with the final commit after stopping, as the Java loop commits,
  // before stopping the clock.
  private def fs2Consumer(topic: String, count: Int)(
    pipe: Records => fs2.Stream[IO, Unit]
  ): Unit =
    run(
      KafkaShareConsumer
        .resource(shareConsumerSettings)
        .use { consumer =>
          consumer.subscribeTo(topic) *>
            pipe(
              consumer.records.through(markFirst).through(distinct(count))
            ).compile.drain *> consumer.stopConsuming *> consumer.awaitTermination *>
            IO { lastAt = System.nanoTime }
        }
    )

  private def acknowledgeChunksPipe(
    work: Option[IO[Unit]]
  )(records: Records): fs2.Stream[IO, Unit] =
    work
      .fold(records.map(r => r.offset -> (AcknowledgeType.Accept: AcknowledgeType))) { w =>
        records.parEvalMapUnordered(parallelism)(r =>
          w.as(r.offset -> (AcknowledgeType.Accept: AcknowledgeType))
        )
      }
      .through(acknowledgeChunks[IO])

  private def acknowledgePerRecord(work: Option[IO[Unit]])(records: Records): fs2.Stream[IO, Unit] =
    work.fold(records.evalMap(_.accept))(w =>
      records.parEvalMapUnordered(parallelism)(r => w *> r.accept)
    )

  // Java's equivalent: a pool processes each poll's records, then the poll thread acknowledges them.
  private def javaConsumer(topic: String, count: Int, work: Option[() => Unit]): Unit = {
    val properties = new Properties()
    shareConsumerSettings.properties.foreach { case (key, value) => properties.put(key, value) }
    val consumer =
      new JavaShareConsumer[String, String](
        properties,
        new StringDeserializer,
        new StringDeserializer
      )
    val pool = Executors.newFixedThreadPool(parallelism)
    try {
      consumer.subscribe(List(topic).asJava)
      val seen     = scala.collection.mutable.HashSet.empty[(String, Int, Long)]
      val deadline = System.nanoTime + 2.minutes.toNanos
      while (seen.size < count) {
        if (System.nanoTime > deadline) sys.error(s"only ${seen.size} of $count records consumed")
        val records = consumer.poll(Duration.ofMillis(100)).asScala.toVector
        if (firstAt == 0L && records.nonEmpty) firstAt = System.nanoTime
        val processed = work.fold(records) { w =>
          records
            .map(r =>
              pool.submit(new Callable[JavaConsumerRecord[String, String]] {
                def call(): JavaConsumerRecord[String, String] = { w(); r }
              })
            )
            .map(_.get())
        }
        processed.foreach { r =>
          consumer.acknowledge(r, JavaAcknowledgeType.ACCEPT)
          seen += ((r.topic, r.partition, r.offset))
        }
      }
      consumer.commitSync(): Unit
      lastAt = System.nanoTime
    } finally {
      pool.shutdown()
      consumer.close()
    }
  }

  private def compare(
    topic: String,
    count: Int,
    label: String,
    work: Option[FiniteDuration]
  ): Unit = {
    val fs2Work = work.map(IO.sleep(_))
    time(s"java, $label", count)(
      javaConsumer(topic, count, work.map(d => () => Thread.sleep(d.toMillis)))
    )
    time(s"fs2 acknowledgeChunks, $label", count)(
      fs2Consumer(topic, count)(acknowledgeChunksPipe(fs2Work))
    )
    time(s"fs2 acknowledge per record, $label", count)(
      fs2Consumer(topic, count)(acknowledgePerRecord(fs2Work))
    )
  }

  describe("throughput") {
    it("should be compared with Java's share consumer") {
      withTopic { topic =>
        val count = 100000
        createCustomTopic(topic).get
        publish(topic, (0 until count).map(_.toString)*)
        // The first runs warm up the JVM.
        compare(topic, count, "sequential, no work (warm-up)", None)
        compare(topic, count, "sequential, no work", None)
        compare(topic, count, s"$parallelism in parallel, 1 ms of work", Some(1.milli))
      }
      withTopic { topic =>
        val count = 10000
        createCustomTopic(topic).get
        publish(topic, (0 until count).map(_.toString)*)
        compare(topic, count, s"$parallelism in parallel, 20 ms of work", Some(20.millis))
      }
    }
  }

}
