# Share Consumers

Share consumers read from a Kafka share group ([KIP-932](https://cwiki.apache.org/confluence/display/KAFKA/KIP-932%3A+Queues+for+Kafka)), using the [Java share consumer][java-share-consumer]. Instead of committing one offset per partition, we acknowledge each delivered record, and several consumers can process records from the same partition.

> **Experimental.** The API may change in minor releases.

Share consumers are in a separate module, which depends on the core module.

```scala
libraryDependencies += "@ORGANIZATION@" %% "@SHARE_MODULE_NAME@" % "@VERSION@"
```

## Prerequisites

- **Brokers with share groups enabled.** The module uses the Kafka @KAFKA_VERSION@ client and is tested against Kafka @KAFKA_VERSION@ brokers. Share groups are turned on by the `share.version` feature, which may be off on a new cluster: `kafka-features.sh --bootstrap-server <broker> upgrade --feature share.version=1`. Renewing locks needs a broker with [KIP-1222](https://cwiki.apache.org/confluence/display/KAFKA/KIP-1222%3A+Acquisition+lock+timeout+renewal+for+share+consumers).

- **Where a new share group starts.** A new share group starts at the latest offset, so records published before it existed are not delivered. To start at the beginning, set the group's `share.auto.offset.reset` to `earliest`, for example with `kafka-configs.sh --bootstrap-server <broker> --alter --entity-type groups --entity-name share-workers --add-config share.auto.offset.reset=earliest`, or with `Admin#incrementalAlterConfigs`.

The following imports are assumed throughout this page.

```scala mdoc:silent
import scala.concurrent.duration._

import cats.effect._
import cats.effect.syntax.all._
import cats.syntax.all._
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

We create @:api(fs2.kafka.ShareConsumerSettings) like `ConsumerSettings`, with the key and value deserializers.

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

- `withMaxProcessingTime` sets how long a record may be held without an acknowledgement before it is released, so a hung record does not hold back its consumer. No limit by default, or with `withoutMaxProcessingTime`; a limit can be at most 365 days. See [renewal](#renewal).

- `withMaxAcknowledgementDelay` sets how long a pending acknowledgement may wait before the consumer sends it, without waiting for the next poll. Default is 1 second; `withoutMaxAcknowledgementDelay` sends acknowledgements only with the next poll, as a Java loop does; a delay can be at most 365 days. See [acknowledging](#acknowledging).

- `withPauseWhen` takes a `Signal[F, Boolean]`; while it is `true`, the consumer acquires no new records. Not paused by default. See [pausing](#pausing).

- `withCommitTimeout` sets how long the final commit after `stopConsuming` waits for the broker to confirm the last acknowledgements. Default is 15 seconds.

- `withCloseTimeout` controls the timeout when waiting for consumer shutdown. Default is 20 seconds.

- `withMaxPollRecords` sets the records polled at once, and so held per consumer with `record_limit`. Default is 500.

- `withPollTimeout` modifies for how long `poll` is allowed to block. The consumer polls again as soon as the previous poll is processed, so this is also how long an idle consumer waits between polls. Default is 50 milliseconds.

## Consuming

A @:api(fs2.kafka.KafkaShareConsumer) runs the same loop you would write with the Java consumer: it polls, gives the poll's records to a `process` function, and polls again once `process` returned. `consume` runs this loop until `stopConsuming`.

Records come as @:api(fs2.kafka.AcknowledgeableConsumerRecord)s. Each contains a `ConsumerRecord` and an @:api(fs2.kafka.AcknowledgeableOffset), which we acknowledge once the record is processed. Within a poll, records can be processed in parallel.

```scala mdoc:silent
def process(record: ConsumerRecord[String, String]): IO[Unit] =
  IO.println(s"${record.key}: ${record.value}")

val program: IO[Unit] =
  KafkaShareConsumer.resource(settings).use { consumer =>
    consumer.subscribeTo("jobs") *>
      consumer.consume { records =>
        records.parTraverseN(32) { record =>
          process(record.record) *> record.accept
        }.void
      }
  }
```

| `AcknowledgeType` | Effect |
| --- | --- |
| `Accept` | The record was processed |
| `Release` | The record is delivered again |
| `Reject` | The record is not delivered again |
| `Renew` | The record's acquisition lock is extended; it needs another acknowledgement later |

`AcknowledgeType#asJava` returns Java's `AcknowledgeType`, and `AcknowledgeableOffset#deliveryCount` is how often the record has been delivered.

Once `process` returns, records it left without an acknowledgement are released, so that the next poll can run, and they are delivered again with a higher delivery count. Acknowledging a record after `process` returned fails with a `RecordNotInFlightException`.

Delivery is at-least-once, so processing must tolerate redelivery. A failed record can be released for another attempt, or rejected if it can never succeed.

```scala mdoc:silent
def processOrRelease(record: AcknowledgeableConsumerRecord[IO, String, String]): IO[Unit] =
  process(record.record).attempt.flatMap {
    case Right(_) => record.accept
    case Left(_)  => record.release
  }
```

If `process` itself fails, the records it left without an acknowledgement are released, and `consume` fails with the same error. The same happens when `consume` is canceled.

## Acknowledging

As in Java, `acknowledge` records the acknowledgement in the consumer, and sends nothing. The next poll sends the pending acknowledgements, so acknowledging every record is enough. As the next poll waits for `process`, which may wait for its slowest record, the consumer also sends pending acknowledgements once the oldest has waited for `withMaxAcknowledgementDelay`, 1 second by default, so that acknowledgements for fast records are not lost with their lock, or with the consumer, while a slow record is processed.

- `record.accept`, `release`, `reject` and `renew` acknowledge one record, as does `acknowledge(AcknowledgeType)`. They are on the record and on its `offset`.

- `acknowledgeChunks` acknowledges the records of each chunk of a stream with one call per consumer, for example `Stream.chunk(records).map(_.offset -> (AcknowledgeType.Accept: AcknowledgeType)).through(acknowledgeChunks[IO])` to accept a whole poll at once.

- There is no commit to call. The consumer sends the pending acknowledgements itself: with the next poll, after the delay, at once while paused, when `consume` ends, and when the consumer is closed.

As in Java, the last acknowledgement for a record wins until it is sent. Once sent, a later acknowledgement fails with a `RecordNotInFlightException`; so does an acknowledgement for a delivery the consumer no longer holds: `process` returned, its renewal failed, the consumer released it after the max processing time, or the offset belongs to an earlier delivery. `acknowledgeChunks` skips these, as the record is delivered again.

Acknowledging succeeds without asking the broker, so an acknowledgement for a record whose lock has expired is only rejected when it is sent. Results are not reported: the record is delivered again. Errors are per partition, so the broker's rejection applies to every acknowledgement of that partition in the same request.

## Deserialization Failures

A record which fails to deserialize fails `consume`, and the poll's records are released. The next consumer fails on it too, until the broker archives it at the delivery count limit. Instead, we can use `attempt` deserializers, which return failures as values, and reject the failed records.

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

A record which is not acknowledged within its acquisition lock, 30 seconds by default, is delivered again, possibly to another consumer, and its later acknowledgement fails. As in Java, records are not renewed automatically. We can renew a record with `Renew`, any number of times before its final acknowledgement.

Java lets a renewed record be acknowledged again only once a poll has returned it, and that poll needs every other record of the batch acknowledged or renewed too. So a renewal here renews every record of the poll still without an acknowledgement, waits for the broker to confirm it, and takes the records back with a poll, which fetches nothing new. `process` keeps the records it has; they are not handed out again. The wait is the renewal margin, at most half of the lock; if the broker has not confirmed by then, the renewed records count as lost, and their later acknowledgements fail with a `RecordNotInFlightException`. Should the broker confirm such a renewal later, Java returns the records from a later poll; the consumer releases them at once rather than handing them out again, so they come back with a higher delivery count.

With `withAutoRenewal(true)`, records without an acknowledgement are renewed `withRenewalMargin` (5 seconds by default) before their lock ends, or earlier when half of `max.poll.interval.ms` requires it. Enable it when processing may outlast the lock.

- **A record's lock counts from the start of the poll which returned it.** Java can fetch records earlier and keep them buffered, for example from a second broker while you process the previous poll; a record buffered for longer than the margin can lose its lock before it is renewed, so raise the margin if polls take long to process.

- **Without auto-renewal, keep processing within the lock** and `max.poll.interval.ms`, as in Java. A record whose lock expired is delivered again, possibly to another consumer, but `process` still holds it, and this consumer does not poll until `process` returns.

- **A record whose processing hangs** holds back its consumer, renewed until the consumer is stopped or closed, unless you set `withMaxProcessingTime`. A record held without an acknowledgement for longer is released and delivered again; the release is sent at once. The time counts from when `process` got the poll, or from your latest `Renew` of the record, so set it well above the time to process a whole poll. Its processing is not cancelled, and its later acknowledgement fails with a `RecordNotInFlightException`.

- **A late acknowledgement fails** with a `RecordNotInFlightException` once the consumer has let go of the record. One for a record whose lock has expired is rejected by the broker when it is sent, see [acknowledging](#acknowledging). Either way, the record is processed again after its redelivery.

```scala mdoc:silent
val renewing: IO[Unit] =
  KafkaShareConsumer
    .resource(settings.withAutoRenewal(true).withMaxProcessingTime(10.minutes))
    .use { consumer =>
      consumer.subscribeTo("jobs") *>
        consumer.consume { records =>
          records.parTraverseN(32)(processOrRelease).void
        }
    }
```

## Pausing

When something the processing needs is unavailable, polling on would acquire records only to fail them, and each redelivery raises their delivery count, until the broker archives them at the delivery count limit. With `withPauseWhen(signal)`, the consumer polls for no new records while the signal is `true`.

```scala mdoc:silent
import fs2.concurrent.SignallingRef

def pausable(paused: SignallingRef[IO, Boolean]): IO[Unit] =
  KafkaShareConsumer.resource(settings.withPauseWhen(paused)).use { consumer =>
    consumer.subscribeTo("jobs") *>
      consumer.consume { records =>
        records.parTraverseN(32)(processOrRelease).void
      }
  }
```

Deciding when to pause is up to us: a circuit breaker, a health check, or failures counted in the processing, which sets the signal. While paused:

- The poll being processed is acknowledged as usual. As no poll comes, acknowledgements are sent at once. Auto-renewal keeps renewing records still held; the polls which return renewed records fetch no new ones, and a new record one still returns, e.g. after a renewal timed out, is released at once.

- With no records held, a pause longer than `max.poll.interval.ms` makes Java leave the share group; the first poll after it rejoins. Nothing is acquired meanwhile, so nothing is lost.

- If the last poll before the pause returned nothing, Java keeps fetching in the background until it acquires records, once. Those records wait in its buffer until their lock expires, and are delivered after the pause with a delivery count one higher.

- `stopConsuming` still finishes, with its final commit.

Pausing stops new records. For a record whose processing fails while the service is down, retry it rather than releasing it: keep it unacknowledged, with auto-renewal holding its lock, so that its delivery count does not grow.

## Acknowledgement and Acquire Modes

| Property | Value | Behaviour |
| --- | --- | --- |
| `share.acknowledgement.mode` | `explicit` (default here) | Every record needs an acknowledgement before the next poll; use `consume` |
| | `implicit` (Java's default) | A poll is accepted once `process` returned; use `autoConsume` |
| `share.acquire.mode` | `record_limit` (default here) | A poll returns at most `max.poll.records` records |
| | `batch_optimized` (Java's default) | A poll may return more, to align with the broker's record batches |

### Implicit Acknowledgement

With implicit acknowledgement, records cannot be acknowledged, and `consume` fails with an `ImplicitAcknowledgementException`. Instead, `autoConsume` gives `process` plain `ConsumerRecord`s, and accepts the poll once `process` returned, as the next poll of a Java loop would. This is at-least-once, like the Java loop: if `process` fails, the poll is not accepted, and closing the consumer releases its records, so they are delivered again. As any later poll would accept them, the consumer then refuses to consume again; close it and create a new one.

```scala mdoc:silent
val implicitly: IO[Unit] =
  KafkaShareConsumer
    .resource(settings.withAcknowledgementMode(ShareAcknowledgementMode.Implicit))
    .use { consumer =>
      consumer.subscribeTo("jobs") *>
        consumer.autoConsume { records =>
          records.parTraverseN(32)(process).void
        }
    }
```

### Acquisition

The consumer polls again only once `process` returned, so a consumer holds at most one poll of records.

- With `record_limit`, a poll returns at most `max.poll.records` records.

- With `batch_optimized`, a poll returns up to `max.poll.records` records plus the rest of the broker record batch it reached, so it can exceed the limit by up to one batch, depending on the producer's `batch.size`.

Java may also fetch records into its buffer before they are polled. Their locks already run while they wait.

A larger `batch_optimized` poll means a slow record holds back more records, and more locks run while records wait, so more renewals. It reads fewer, larger batches, which can help throughput when processing is fast and uniform.

## Several Consumers

To keep polling past slow records, we can use several consumers in the same share group. Each has its own Java consumer and its own loop, so polls are processed in parallel across consumers, one at a time within each, and the broker distributes records between them.

```scala mdoc:silent
val parallelProgram: IO[Unit] =
  KafkaShareConsumer.resource(settings, consumers = 4).use { consumers =>
    consumers.subscribeTo("jobs") *>
      consumers.consume { records =>
        records.parTraverseN(8)(processOrRelease).void
      }
  }
```

`consume` runs on every consumer until `stopConsuming`. If one of them fails, the others are canceled, which releases the records they hold, and the error is raised. When `client.id` is set, consumer `i` uses `client.id-i`.

## Stopping and Shutdown

- `stopConsuming` makes `consume` return once the poll being processed is done and a final commit sent the last acknowledgements. If that commit fails, `consume` fails with its error. Stopping is final: consuming again fails; create a new consumer instead.

- Releasing the resource closes the Java consumer, which sends the pending acknowledgements and releases the records it still holds without one; it never accepts them automatically.

[java-share-consumer]: @KAFKA_API_BASE_URL@/org/apache/kafka/clients/consumer/KafkaShareConsumer.html
