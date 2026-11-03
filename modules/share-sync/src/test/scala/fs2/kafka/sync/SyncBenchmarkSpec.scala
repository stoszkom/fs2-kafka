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
import fs2.kafka.*

/**
  * Throughput of the synchronous consumer against the actor, on the same topic and workloads. Like
  * `ShareBenchmarkSpec`, it runs only when `SHARE_BENCHMARK` is set:
  * {{{
  * SHARE_BENCHMARK=1 sbt "share-sync/testOnly fs2.kafka.sync.SyncBenchmarkSpec"
  * }}}
  */
final class SyncBenchmarkSpec extends ShareConsumerBaseKafkaSpec {

  override def runTest(testName: String, args: org.scalatest.Args): org.scalatest.Status =
    if (sys.env.contains("SHARE_BENCHMARK")) super.runTest(testName, args)
    else org.scalatest.SucceededStatus

  private val parallelism = 32

  // From the first record to the end, after the final commit, as records per second.
  private def rate(count: Int)(consume: IO[Unit] => IO[Unit]): IO[Double] =
    Ref
      .of[IO, Option[Long]](None)
      .flatMap { first =>
        val mark = first.update(_.orElse(Some(System.nanoTime)))
        consume(mark) *> (IO(System.nanoTime), first.get).mapN((end, start) =>
          count / ((end - start.get) / 1e9)
        )
      }

  private def actor(topic: String, count: Int, settings: ShareConsumerSettings[IO, String, String])(
    work: Option[FiniteDuration]
  ): IO[Double] =
    rate(count) { mark =>
      KafkaShareConsumer
        .resource(settings)
        .use { consumer =>
          val records = consumer.records.evalTap(_ => mark)
          val done    = work.fold(records.evalMap(_.accept))(w =>
            records.parEvalMapUnordered(parallelism)(r => IO.sleep(w) *> r.accept)
          )
          consumer.subscribeTo(topic) *> done.take(count.toLong).compile.drain *>
            consumer.stopConsuming *> consumer.awaitTermination
        }
    }

  private def sync(topic: String, count: Int, settings: ShareConsumerSettings[IO, String, String])(
    work: Option[FiniteDuration]
  ): IO[Double] =
    rate(count) { mark =>
      SyncShareConsumer
        .resource(settings)
        .use { consumer =>
          Ref
            .of[IO, Int](0)
            .flatMap { accepted =>
              def done(n: Int) = accepted
                .updateAndGet(_ + n)
                .flatMap(a => consumer.stopConsuming.whenA(a >= count))
              consumer.subscribeTo(topic) *> consumer.consume { records =>
                mark *> work.fold(records.traverse_(_.accept))(w =>
                  records.parTraverseN(parallelism)(r => IO.sleep(w) *> r.accept).void
                ) *> done(records.size)
              }
            }
        }
    }

  private def compare(
    topic: String,
    count: Int,
    label: String,
    work: Option[FiniteDuration]
  ): Unit =
    List("actor" -> (actor _), "sync" -> (sync _)).foreach { case (name, consumer) =>
      val perSecond = run(consumer(topic, count, shareConsumerSettings)(work))
      println(f"BENCHMARK $name%-6s $label%-36s $perSecond%10.0f records/s")
    }

  describe("throughput") {
    it("should be compared between the actor and the synchronous consumer") {
      withTopic { topic =>
        val count = 100000
        createCustomTopic(topic).get
        publish(topic, (0 until count).map(_.toString)*)
        compare(topic, count, "per record, no work (warm-up)", None)
        compare(topic, count, "per record, no work", None)
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
