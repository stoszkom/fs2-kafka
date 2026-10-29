---
id: share-consumers
title: Share Consumers
---

Share consumers read from a Kafka share group ([KIP-932](https://cwiki.apache.org/confluence/display/KAFKA/KIP-932%3A+Queues+for+Kafka)), using the [Java share consumer][java-share-consumer]. Instead of committing one offset per partition, we acknowledge each delivered record, and several consumers can process records from the same partition.

> **Experimental.** The API may change in minor releases. It requires Kafka 4.x brokers with share groups enabled, and renewing acquisition locks requires [KIP-1222](https://cwiki.apache.org/confluence/display/KAFKA/KIP-1222%3A+Acquisition+lock+timeout+renewal+for+share+consumers).

Share consumers are in a separate module, which depends on the core module.

```scala
libraryDependencies += "@ORGANIZATION@" %% "@SHARE_MODULE_NAME@" % "@LATEST_VERSION@"
```

The following imports are assumed throughout this page.

```scala mdoc:silent
import scala.concurrent.duration._

import cats.effect._
import fs2._
import fs2.kafka._
```

## When to Use Share Consumers

A share group makes a topic behave like a queue. A consumer group assigns each partition to one consumer, which processes it in order. A share group instead hands out individual records to whichever consumer polls, tracks the delivery of each record, and delivers it again when it is released or its lock expires.

Share consumers fit best when:

- **records are independent jobs**, such as tasks, notifications or calls to an external service, and their order does not matter,

- **more consumers are needed than there are partitions**, as consumers can be added or removed without repartitioning the topic,

- **processing time varies**, as a slow record holds back only its own consumer, not a whole partition, and

- **records are retried one at a time**. A failed record can be released for another attempt without affecting the others. `deliveryCount` tells how often it was delivered, and the broker archives it at the share group's delivery count limit, 5 by default. `Reject` stops it from being delivered again.

A regular [`KafkaConsumer`](consumers.md) fits better when:

- **order matters**, as share groups give no ordering guarantee, not even per partition or key,

- **exactly-once processing is needed**, as share groups do not take part in transactions,

- **processing depends on the partition**, such as state per key or seeking to an offset, or

- **processing is fast and uniform**, as committing one offset per partition is less work than acknowledging every record.

## Settings

We create [`ShareConsumerSettings`][sharesettings] like `ConsumerSettings`, with the key and value deserializers.

```scala mdoc:silent
val settings = ShareConsumerSettings[IO, String, String]
  .withBootstrapServers("localhost:9092")
  .withGroupId("share-workers")
  .withMaxPollRecords(100)
```

The following settings differ from Java's share consumer defaults. Both are Java properties, set with `withProperty`; see [acknowledgement and acquire modes](#acknowledgement-and-acquire-modes).

- `share.acknowledgement.mode` is set to `explicit`, so every record needs an acknowledgement. Java's default is `implicit`.

- `share.acquire.mode` is set to `record_limit`, so a poll returns at most `max.poll.records` records. Java's default is `batch_optimized`.

In addition, there are several settings specific to the library.

- `withAutoRenewal` renews records without an acknowledgement before their lock expires. Default is `false`, as in Java. See [renewal](#renewal).

- `withCloseTimeout` controls the timeout when waiting for consumer shutdown. Default is 20 seconds.

- `withCommitTimeout` sets how long to wait for the broker to confirm a commit. Default is 15 seconds. With auto-renewal, it must leave time to renew behind a commit: when `lock - commitTimeout - margin <= 0`, where `margin = min(lock / 10, 1 second)`, the stream fails with a `ConfigException`. The default suits locks of about 17 seconds and longer.

- `withMaxPollRecords` sets the records polled at once, and so held per consumer with `record_limit`. Default is 500.

- `withPollInterval` alters how often `poll` takes place. It is also the renewal interval until the broker reports its lock duration, and caps the waits for commit confirmations. Default is 50 milliseconds.

- `withPollTimeout` modifies for how long `poll` is allowed to block. Default is 50 milliseconds.

## Record Streaming

Once subscribed, `records` gives a `Stream` of [`AcknowledgeableConsumerRecord`][acknowledgeablerecord]s, in the order they were polled. Each contains a `ConsumerRecord` and an [`AcknowledgeableOffset`][acknowledgeableoffset], which we acknowledge once the record is processed.

```scala mdoc:silent
def process(record: ConsumerRecord[String, String]): IO[Unit] =
  IO.println(s"${record.key}: ${record.value}")

val program: IO[Unit] =
  KafkaShareConsumer.stream(settings)
    .subscribeTo("jobs")
    .records
    .parEvalMapUnordered(32) { committable =>
      process(committable.record).as(committable.offset.acknowledge(AcknowledgeType.Accept))
    }
    .through(commitPollWithin(1.second))
    .compile
    .drain
```

| `AcknowledgeType` | Effect |
| --- | --- |
| `Accept` | The record was processed |
| `Release` | The record is delivered again |
| `Reject` | The record is not delivered again |
| `Renew` | The record's acquisition lock is extended; it needs another acknowledgement later |

`AcknowledgeType#asJava` returns Java's `AcknowledgeType`, and `AcknowledgeableOffset#deliveryCount` is how often the record has been delivered.

The record stream of a consumer can be started only once, otherwise it fails with a `StreamAlreadyStartedException`. To consume again, create a new consumer. Unlike with `KafkaConsumer`, a new consumer does not hold back the others, as share groups do not assign partitions exclusively.

## Offset Commits

As in Java, acknowledging and committing are separate steps.

- `offset.acknowledge(AcknowledgeType)` returns an `AcknowledgedOffset` value, and sends nothing.

- `AcknowledgedOffset#commit` commits one offset. `AcknowledgedOffsetBatch#commit` commits several, with one call per consumer.

- `commitPollWithin(maxDelay)` commits in batches, per consumer: as soon as the offsets cover the consumer's latest poll, so it can poll again, and at most `maxDelay` after the oldest offset arrived. Offsets waiting longer than the renewal interval, about half the lock, are renewed meanwhile, which costs a request and delays their commit. Without auto-renewal, keep `maxDelay` below the lock, or waiting offsets lose it.

- `KafkaShareConsumer#commitAsync` and `commitSync` take offsets by topic and partition, with their acknowledge types.

A commit completes once the broker has confirmed it, and fails with the first error the broker reported. `AcknowledgedOffsetBatch#commitWithResults` returns the result per partition instead, as a `Map[TopicPartition, Option[KafkaException]]`. It still fails when the whole commit fails, for example with a `ShareCommitTimeoutException`.

Errors are reported per partition, so an error fails every acknowledgement of that partition in the same request, even ones that may have been applied.

- `RecordNotInFlightException` means the consumer no longer holds the delivery: it was already acknowledged, its renewal failed, or the offset belongs to an earlier delivery.

- `InvalidRecordStateException`, from the broker, means the record's lock has expired. The consumer does not track lock expiry itself.

- `ShareCommitTimeoutException` means the broker did not confirm within the commit timeout. The acknowledgements may still have been applied.

Delivery is at-least-once, so processing must tolerate redelivery. A failed record can be released for another attempt.

```scala mdoc:silent
def processOrRelease(committable: AcknowledgeableConsumerRecord[IO, String, String]): IO[Unit] =
  process(committable.record).attempt.flatMap {
    case Right(_) => committable.offset.acknowledge(AcknowledgeType.Accept).commit
    case Left(_)  => committable.offset.acknowledge(AcknowledgeType.Release).commit
  }
```

## Deserialization Failures

A record which fails to deserialize fails the stream and the consumer. Closing the consumer releases the record, so the next consumer fails on it too, until the broker archives it at the delivery count limit. Instead, we can use `attempt` deserializers, which return failures as values, and reject the failed records.

```scala mdoc:silent
val attemptSettings =
  ShareConsumerSettings(
    keyDeserializer = Deserializer[IO, String],
    valueDeserializer = Deserializer[IO, String].attempt
  ).withBootstrapServers("localhost:9092")
    .withGroupId("share-workers")

def processOrReject(
  committable: AcknowledgeableConsumerRecord[IO, String, Either[Throwable, String]]
): IO[Unit] =
  committable.record.value match {
    case Right(value) =>
      IO.println(value) *> committable.offset.acknowledge(AcknowledgeType.Accept).commit
    case Left(_) =>
      committable.offset.acknowledge(AcknowledgeType.Reject).commit
  }
```

## Renewal

A record which is not acknowledged within its acquisition lock, 30 seconds by default, is delivered again, possibly to another consumer, and its later acknowledgement fails. As in Java, records are not renewed automatically. We can renew a record with `Renew`, any number of times before its final acknowledgement.

With `withAutoRenewal(true)`, records without an acknowledgement are renewed at about half of the lock duration, or earlier when the commit timeout or half of `max.poll.interval.ms` requires it. Enable it when processing may outlast the lock. Note that a record whose processing hangs is then renewed, and holds back its consumer, until the stream ends.

- **A slow record holds back its consumer.** Renewing lets the consumer poll again, but Java then returns only the renewed records, and polls new ones only once every record is settled. No setting changes this. Use [several consumers](#several-consumers) to keep polling past slow records.

- **A renewed record is acknowledged after the next poll.** That poll needs every other record of the batch acknowledged. With auto-renewal, the consumer renews the other records right away, so it can poll. Without it, the acknowledgement waits until every other record is acknowledged or renewed, so do not hold those back while waiting for it.

- **Without auto-renewal, the consumer does not poll** while a record has no acknowledgement, so keep processing within `max.poll.interval.ms`.

## Acknowledgement and Acquire Modes

| Property | Value | Behaviour |
| --- | --- | --- |
| `share.acknowledgement.mode` | `explicit` (default here) | Every record needs an acknowledgement before the next poll |
| | `implicit` (Java's default) | The next poll, or a commit, accepts every record of the previous poll; acknowledging fails with an `ImplicitAcknowledgementException` |
| `share.acquire.mode` | `record_limit` (default here) | A poll returns at most `max.poll.records` records |
| | `batch_optimized` (Java's default) | A poll may return more, to align with the broker's record batches |

### Delivery Guarantees

- **Explicit acknowledgement** is at-least-once when we acknowledge after processing, as above: a record which is not acknowledged is delivered again. Committing `Accept` before processing gives at-most-once.

- **Implicit acknowledgement** is at-most-once here. In a Java poll loop, the next poll comes after processing. A stream polls in the background instead, once every record of the previous poll has been handed out, so records may still be processing, for example in `parEvalMapUnordered`, when they are accepted. A record whose processing fails is not delivered again, similar to `withEnableAutoCommit(true)` on `KafkaConsumer`. Use explicit acknowledgement for at-least-once.

With implicit acknowledgement, records are not renewed, as Java does not allow it, and `commitPollWithin`, `AcknowledgedOffsetBatch`, `commitAsync` and `commitSync` fail with an `ImplicitAcknowledgementException`. After `stopConsuming`, the last records are accepted once handed out. When the stream ends before a poll was fully handed out, that poll is not accepted, and closing the consumer releases its records.

```scala mdoc:silent
val atMostOnce: IO[Unit] =
  KafkaShareConsumer
    .stream(settings.withProperty("share.acknowledgement.mode", "implicit"))
    .subscribeTo("jobs")
    .records
    .parEvalMapUnordered(32)(committable => process(committable.record))
    .compile
    .drain
```

### Acquisition

The consumer polls again only once the previous poll is acknowledged (explicit) or handed out (implicit), so it never queues more than one poll.

- With `record_limit`, a poll returns at most `max.poll.records` records.

- With `batch_optimized`, a poll returns up to `max.poll.records` records plus the rest of the broker record batch it reached, so it can exceed the limit by up to one batch, depending on the producer's `batch.size`.

In explicit mode, a consumer holds at most one poll of records. In implicit mode, handed-out records are no longer tracked, so downstream may still process earlier records, for example up to 32 in `parEvalMapUnordered(32)`.

Java may also fetch records into its buffer before they are polled. Their locks already run while they wait.

A larger `batch_optimized` poll means a slow record holds back more records, and more locks run while records wait, so more renewals. It reads fewer, larger batches, which can help throughput when processing is fast and uniform.

## Several Consumers

To keep polling past slow records, we can use several consumers in the same share group. Each has its own Java consumer, and the broker distributes records between them.

```scala mdoc:silent
val parallelProgram: IO[Unit] =
  KafkaShareConsumer.resource(settings, consumers = 4).use { consumers =>
    consumers.subscribeTo("jobs") *>
      consumers
        .records
        .parEvalMapUnordered(32) { committable =>
          process(committable.record).as(committable.offset.acknowledge(AcknowledgeType.Accept))
        }
        .through(commitPollWithin(1.second))
        .compile
        .drain
  }
```

- `records` merges the records of all consumers into one pipeline.

- `perConsumerRecords` gives one stream per consumer, so slow records only occupy their own consumer's pipeline.

- `KafkaShareConsumer.stream(settings, consumers)(subscribe)` subscribes each consumer and streams the merged records.

When `client.id` is set, consumer `i` uses `client.id-i`.

## Graceful Shutdown


- `stopConsuming` stops polling. The stream ends once every polled record is acknowledged.

- When the stream ends, records it never handed out are released. Operators such as `prefetch` count records as handed out. With auto-renewal, handed-out records are renewed until acknowledged.

- Releasing the resource closes the Java consumer, which releases the records it still holds, and never accepts them automatically. Later commits fail with a `ConsumerShutdownException`.

## Testing Against a Real Broker

`KafkaShareConsumerSpec` starts a Kafka 4.3 broker with testcontainers and enables share groups on it. It needs Docker.

```sh
sbt 'share/testOnly fs2.kafka.KafkaShareConsumerSpec'
```

[acknowledgeableoffset]: @API_BASE_URL@/AcknowledgeableOffset.html
[acknowledgeablerecord]: @API_BASE_URL@/AcknowledgeableConsumerRecord.html
[java-share-consumer]: @KAFKA_API_BASE_URL@/org/apache/kafka/clients/consumer/KafkaShareConsumer.html
[sharesettings]: @API_BASE_URL@/ShareConsumerSettings.html
