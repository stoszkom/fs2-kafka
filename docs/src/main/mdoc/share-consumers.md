---
id: share-consumers
title: Share Consumers
---

Share consumers read from a Kafka share group ([KIP-932](https://cwiki.apache.org/confluence/display/KAFKA/KIP-932%3A+Queues+for+Kafka)), using the [Java share consumer][java-share-consumer]. Instead of committing one offset per partition, we acknowledge each delivered record, and several consumers can process records from the same partition.

> **Experimental.** The API may change in minor releases.

Share consumers are in a separate module, which depends on the core module.

```scala
libraryDependencies += "@ORGANIZATION@" %% "@SHARE_MODULE_NAME@" % "@LATEST_VERSION@"
```

## Prerequisites

- **Brokers with share groups enabled.** The module uses the Kafka @KAFKA_VERSION@ client and is tested against Kafka 4.3 brokers. Share groups are turned on by the `share.version` feature, which may be off on a new cluster: `kafka-features.sh --bootstrap-server <broker> upgrade --feature share.version=1`. Renewing locks needs a broker with [KIP-1222](https://cwiki.apache.org/confluence/display/KAFKA/KIP-1222%3A+Acquisition+lock+timeout+renewal+for+share+consumers).

- **Where a new share group starts.** A new share group starts at the latest offset, so records published before it existed are not delivered. To start at the beginning, set the group's `share.auto.offset.reset` to `earliest`, for example with `kafka-configs.sh --bootstrap-server <broker> --alter --entity-type groups --entity-name share-workers --add-config share.auto.offset.reset=earliest`, or with `Admin#incrementalAlterConfigs`.

The following imports are assumed throughout this page.

```scala mdoc:silent
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

The following settings differ from Java's share consumer defaults. Both are Java properties, set with `withAcknowledgementMode` and `withAcquireMode`; see [acknowledgement and acquire modes](#acknowledgement-and-acquire-modes).

- `share.acknowledgement.mode` is set to `explicit` (`ShareAcknowledgementMode.Explicit`), so every record needs an acknowledgement. Java's default is `implicit`.

- `share.acquire.mode` is set to `record_limit` (`ShareAcquireMode.RecordLimit`), so a poll returns at most `max.poll.records` records. Java's default is `batch_optimized`.

In addition, there are several settings specific to the library.

- `withAutoRenewal` renews records without an acknowledgement before their lock expires. Default is `false`, as in Java. See [renewal](#renewal).

- `withRenewalMargin` sets how long before a record's lock ends auto-renewal renews it. Default is 5 seconds. See [renewal](#renewal).

- `withCloseTimeout` controls the timeout when waiting for consumer shutdown. Default is 20 seconds.

- `withCommitTimeout` sets how long to wait for the broker to confirm a commit. Default is 15 seconds. With auto-renewal, it must be shorter than the lock, with a small margin, or the stream fails with a `ConfigException`; the default suits locks of about 17 seconds and longer.

- `withMaxProcessingTime` sets how long a record may be held without a decision before it is released, so a hung record does not hold back its consumer. No limit by default, or with `withoutMaxProcessingTime`; a limit can be at most 365 days. See [renewal](#renewal).

- `withMaxAcknowledgementDelay` sets how long a staged decision may wait before the consumer sends it, without waiting for the next poll. Default is 1 second; `withoutMaxAcknowledgementDelay` sends decisions only with the next poll or commit, as in Java; a delay can be at most 365 days. See [acknowledging](#acknowledging).

- `withMaxPollRecords` sets the records polled at once, and so held per consumer with `record_limit`. Default is 500.

- `withPollTimeout` modifies for how long `poll` is allowed to block. The consumer polls again as soon as every record of the previous poll has a decision and the stream has taken it, so there is no poll interval. Default is 50 milliseconds.

## Record Streaming

Once subscribed, `records` gives a `Stream` of [`AcknowledgeableConsumerRecord`][acknowledgeablerecord]s, in the order they were polled. Each contains a `ConsumerRecord` and an [`AcknowledgeableOffset`][acknowledgeableoffset], which we acknowledge once the record is processed.

```scala mdoc:silent
def process(record: ConsumerRecord[String, String]): IO[Unit] =
  IO.println(s"${record.key}: ${record.value}")

val program: IO[Unit] =
  KafkaShareConsumer.stream(settings)
    .subscribeTo("jobs")
    .records
    .parEvalMapUnordered(32) { record =>
      process(record.record) *> record.accept
    }
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

## Acknowledging

As in Java, `acknowledge` stages a decision in the consumer, and sends nothing. The consumer polls again once every record of its latest poll has a decision, and that poll sends the staged decisions, so acknowledging every record is enough. As the next poll waits for the slowest record, the consumer also sends staged decisions once the oldest has waited for `withMaxAcknowledgementDelay`, 1 second by default, so that decisions for fast records are not lost with their lock, or with the consumer, while a slow record is processed.

- `record.accept`, `release`, `reject` and `renew` stage one decision, as does `acknowledge(AcknowledgeType)`. They are on the record and on its `offset`.

- `acknowledgeChunks` stages the decisions of each chunk with one call per consumer. Use it after steps which keep the stream's chunks, such as `map` or `evalMapChunk`, where a chunk is a whole poll: acknowledging one record at a time there waits for the consumer on every record. After `parEvalMap`, chunks mostly hold one record, so acknowledge each record instead; the consumer already stages decisions arriving together in one call. To use `acknowledgeChunks` there anyway, group the decisions first with `groupWithin(n, d).unchunks`.

- `KafkaShareConsumer#commitSync` sends every staged decision at once and waits for the result per partition, as a `Map[TopicPartition, Option[KafkaException]]`, as Java's `commitSync` does. It waits for the commit timeout, or for the timeout given to `commitSync(timeout)`. A partition the broker does not confirm in time has Kafka's `TimeoutException` as its result; a commit that times out before it is sent fails with a `ShareCommitTimeoutException`.

- `KafkaShareConsumer#commitAsync` sends every staged decision without waiting, as Java's `commitAsync` does, and reports no results.

```scala mdoc:silent
val chunked: IO[Unit] =
  KafkaShareConsumer.stream(settings)
    .subscribeTo("jobs")
    .records
    .evalMapChunk { record =>
      process(record.record).as(record.offset -> (AcknowledgeType.Accept: AcknowledgeType))
    }
    .through(acknowledgeChunks[IO])
    .compile
    .drain
```

As in Java, the last decision for a record wins until it is sent. Once sent, a later decision fails with a `RecordNotInFlightException`; so does a decision for a delivery the consumer no longer holds: its renewal failed, the consumer released it after the max processing time, or the offset belongs to an earlier delivery. `acknowledgeChunks` skips these, as the record is delivered again.

Staging succeeds without asking the broker, so a decision for a record whose lock has expired is only rejected when it is sent. The broker's `InvalidRecordStateException` then appears in the results of `commitSync`; a poll, `commitAsync`, or a send after the max acknowledgement delay, sends decisions without reporting results. Errors are reported per partition, so an error applies to every decision of that partition in the same request.

Delivery is at-least-once, so processing must tolerate redelivery. A failed record can be released for another attempt.

```scala mdoc:silent
def processOrRelease(record: AcknowledgeableConsumerRecord[IO, String, String]): IO[Unit] =
  process(record.record).attempt.flatMap {
    case Right(_) => record.accept
    case Left(_)  => record.release
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
  record: AcknowledgeableConsumerRecord[IO, String, Either[Throwable, String]]
): IO[Unit] =
  record.record.value match {
    case Right(value) =>
      IO.println(value) *> record.accept
    case Left(_) =>
      record.reject
  }
```

## Renewal

A record which is not acknowledged within its acquisition lock, 30 seconds by default, is delivered again, possibly to another consumer, and its later acknowledgement fails. As in Java, records are not renewed automatically, except to let a renewed record be acknowledged, see below. We can renew a record with `Renew`, any number of times before its final acknowledgement.

With `withAutoRenewal(true)`, records without an acknowledgement are renewed `withRenewalMargin` (5 seconds by default) before their lock ends, or earlier when the commit timeout or half of `max.poll.interval.ms` requires it. A record's lock counts from the start of the poll which returned it. Java can fetch records earlier and keep them buffered, for example from a second broker while you process the previous poll; a record buffered for longer than the margin can lose its lock before it is renewed, so raise the margin if polls take long to process. Enable it when processing may outlast the lock. Note that a record whose processing hangs is then renewed, and holds back its consumer, until the consumer is closed or stopped, unless you set a max processing time.

- **A slow record holds back its consumer.** Renewing lets the consumer poll again, but Java then returns only the renewed records, and polls new ones only once every record is settled. Use [several consumers](#several-consumers) to keep polling past slow records; `withMaxProcessingTime` only lets go of records that take too long.

- **A renewed record is acknowledged after the next poll**, as Java lets you decide on it again only once a poll has returned it. A decision made before that completes once the poll returns the record and Java staged the decision, usually within one poll, so closing the consumer afterwards still sends it. Until then, the consumer promptly renews the other records of the poll so that it can poll, also without auto-renewal, as Java polls only once every record has a decision. So each decision made right after a `Renew` renews the rest of the poll: renewing every record by hand in a parallel pipeline, such as `parEvalMap(n)(r => r.renew *> process(r) *> r.accept)`, renews the whole poll once per record. For long processing, use auto-renewal, which renews the records together, or renew them in batches, as you would in Java. If the renewal fails, the decision fails with a `RecordNotInFlightException`, as the record is delivered again.

- **Without auto-renewal, the consumer does not poll** while a record has no acknowledgement, as in Java, so keep processing within the lock and `max.poll.interval.ms`. A record whose lock expired is delivered again, possibly to another consumer, but holds back this consumer until it is decided, or until the max processing time releases it.

- **A record whose processing hangs** can be let go with `withMaxProcessingTime`. A record held without a decision for longer is released and delivered again, and the consumer can poll again once the stream has taken its previous poll. Records of a poll the stream has not taken yet are not released. The time counts from when the stream took the record's poll, or from your latest `Renew` of it, until its decision reaches the consumer, so set it well above the time to process a whole poll. With several consumers merged into one pipeline, or `batch_optimized` polls above `max.poll.records`, records wait behind more records, so allow for that too. Its processing is not cancelled.

- **A late decision fails** with a `RecordNotInFlightException` once the consumer has released the record. One for a record whose lock has expired is rejected by the broker when it is sent, see [acknowledging](#acknowledging). Either way, the record is processed again after its redelivery.

## Acknowledgement and Acquire Modes

| Property | Value | Behaviour |
| --- | --- | --- |
| `share.acknowledgement.mode` | `explicit` (default here) | Every record needs an acknowledgement before the next poll |
| | `implicit` (Java's default) | The next poll, or a commit, accepts every record of the previous poll; acknowledging fails with an `ImplicitAcknowledgementException` |
| `share.acquire.mode` | `record_limit` (default here) | A poll returns at most `max.poll.records` records |
| | `batch_optimized` (Java's default) | A poll may return more, to align with the broker's record batches |

### Delivery Guarantees

- **Explicit acknowledgement** is at-least-once when we acknowledge after processing, as above: a record which is not acknowledged is delivered again. Acknowledging `Accept` before processing gives at-most-once.

- **Implicit acknowledgement** is at-most-once here. In a Java poll loop, the next poll comes after processing. A stream polls in the background instead, once it has taken the previous poll, so records may still be processing, for example in `parEvalMapUnordered`, when they are accepted. A record whose processing fails is not delivered again, similar to `withEnableAutoCommit(true)` on `KafkaConsumer`. Use explicit acknowledgement for at-least-once.

With implicit acknowledgement, records are not renewed, as Java does not allow it, `acknowledge` and `acknowledgeChunks` fail with an `ImplicitAcknowledgementException`, and `commitSync` and `commitAsync` accept the latest poll once the stream has taken it. As the consumer polls again once the stream took a poll, and that poll accepts the one before, a commit usually finds nothing to accept, and `commitSync` returns an empty result. After `stopConsuming`, the last poll is accepted once the stream has taken it. A poll the stream never took is not accepted, and closing the consumer releases its records; a poll counts as taken once the stream asks for the records after it, so records skipped by `take` are not accepted.

```scala mdoc:silent
val atMostOnce: IO[Unit] =
  KafkaShareConsumer
    .stream(settings.withAcknowledgementMode(ShareAcknowledgementMode.Implicit))
    .subscribeTo("jobs")
    .records
    .parEvalMapUnordered(32)(record => process(record.record))
    .compile
    .drain
```

### Acquisition

The consumer polls again only once the stream has taken the previous poll and, in explicit mode, every record of it has a decision, so it never queues more than one poll.

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
        .parEvalMapUnordered(32) { record =>
          process(record.record) *> record.accept
        }
        .compile
        .drain
  }
```

- `records` merges the records of all consumers into one pipeline.

- `perConsumerRecords` gives one stream per consumer, so slow records only occupy their own consumer's pipeline.

- `KafkaShareConsumer.stream(settings, consumers)(subscribe)` subscribes each consumer and streams the merged records.

When `client.id` is set, consumer `i` uses `client.id-i`.

## Logging

The consumer logs with SLF4J, under `fs2.kafka.KafkaShareConsumer$<id>`, the same id as the consumer's `toString`: polls that return records at debug level, and records released for exceeding the max processing time as warnings.

## Graceful Shutdown

- `stopConsuming` stops polling. The stream ends once every polled record has a decision, after a final commit sends them; if the stream already ended, records still without a decision are released; a decision that arrives later fails with a `RecordNotInFlightException`. If that final commit fails, the stream fails, and so does a later `commitSync` or `commitAsync`, with its error.

- When the stream ends, records it never handed out are released. In explicit acknowledgement mode, the records of a poll are handed out as one chunk, so operators such as `take` or `prefetch` count the whole chunk as handed out; closing the consumer releases the records they skipped. Auto-renewal continues after the stream ends, as steps such as `parEvalMap` may still process records, until the consumer is closed or stopped.

- Releasing the resource closes the Java consumer, which sends the staged decisions and releases the records it still holds without one; it never accepts them automatically. A record stream still running ends normally, as in core, and later decisions and commits fail with a `ConsumerShutdownException`.

[acknowledgeableoffset]: @API_BASE_URL@/AcknowledgeableOffset.html
[acknowledgeablerecord]: @API_BASE_URL@/AcknowledgeableConsumerRecord.html
[java-share-consumer]: @KAFKA_API_BASE_URL@/org/apache/kafka/clients/consumer/KafkaShareConsumer.html
[sharesettings]: @API_BASE_URL@/ShareConsumerSettings.html
