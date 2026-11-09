# Share Consumers

Share consumers read from a Kafka share group ([KIP-932](https://cwiki.apache.org/confluence/display/KAFKA/KIP-932%3A+Queues+for+Kafka)), using the [Java share consumer][java-share-consumer]. Instead of committing one offset per partition, we acknowledge each delivered record, and several consumers can process records from the same partition.

> **Experimental.** The module targets Kafka's share groups, introduced by KIP-932.

Share consumers are in a separate module.

```scala
libraryDependencies += "@ORGANIZATION@" %% "@SHARE_MODULE_NAME@" % "@VERSION@"
```

## Prerequisites

- **Brokers with share groups enabled.** Share groups are turned on by the `share.version` feature, which may be off on a new cluster: `kafka-features.sh --bootstrap-server <broker> upgrade --feature share.version=1`. Renewing locks needs a broker with [KIP-1222](https://cwiki.apache.org/confluence/display/KAFKA/KIP-1222%3A+Acquisition+lock+timeout+renewal+for+share+consumers).

- **Where a new share group starts.** A new share group starts at the latest offset. To start at the beginning, set the group's `share.auto.offset.reset` to `earliest`, for example with `kafka-configs.sh --bootstrap-server <broker> --alter --entity-type groups --entity-name share-workers --add-config share.auto.offset.reset=earliest`.

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

A share group makes a topic behave like a queue: instead of assigning each partition to one consumer, it hands out individual records to whichever consumer polls, and delivers a record again when it is released or its lock expires.

Share consumers fit best when:

- **records are independent jobs**, and their order does not matter,

- **more consumers are needed than there are partitions**,

- **processing time varies**, as a slow record holds back only its own consumer, and

- **records are retried one at a time**, without affecting the others.

A regular [`KafkaConsumer`](consumers.md) fits better when:

- **order matters**, as share groups give no ordering guarantee, not even per partition,

- **exactly-once processing is needed**, as share groups do not take part in transactions,

- **processing depends on the partition**, such as state per key or seeking, or

- **processing is fast and uniform**, as committing one offset per partition is less work than acknowledging every record.

## Settings

We create @:api(fs2.kafka.ShareConsumerSettings) like `ConsumerSettings`, with the key and value deserializers.

```scala mdoc:silent
val settings = ShareConsumerSettings[IO, String, String]
  .withBootstrapServers("localhost:9092")
  .withGroupId("share-workers")
  .withMaxPollRecords(100)
```

Unlike Java, the acknowledgement mode defaults to `explicit` and the acquire mode to `record_limit`, see [acknowledgement and acquire modes](#acknowledgement-and-acquire-modes). The settings specific to the library are:

- `withAutoRenewal` renews records without an acknowledgement before their lock expires. Default is `false`. See [renewal](#renewal).

- `withRenewalMargin` sets how long before a record's lock ends auto-renewal renews it. Default is 5 seconds.

- `withMaxProcessingTime` limits how long a record may be held without an acknowledgement. No limit by default. See [renewal](#renewal).

- `withFailureAcknowledgement` sets how the consumer acknowledges the records it settles itself, `Release` or `Reject`. Default is `Release`. See [consuming](#consuming).

- `withMaxAcknowledgementDelay` sets how long a pending acknowledgement may wait before it is sent. Default is 1 second. See [acknowledging](#acknowledging).

- `withPauseWhen` pauses polling while a `Signal[F, Boolean]` is `true`. See [pausing](#pausing).

- `withCommitTimeout` sets how long stopping waits for the broker to confirm the last acknowledgements. Default is 15 seconds.

- `withCloseTimeout`, `withMaxPollRecords` and `withPollTimeout` work as for `ConsumerSettings`. Defaults are 20 seconds, 500 and 50 milliseconds.

## Consuming

A @:api(fs2.kafka.KafkaShareConsumer) runs the loop you would write with the Java consumer: it polls, gives the poll's records to a `process` function, and polls again once `process` returned. `consume` runs this loop until `stopConsuming`.

Records come as @:api(fs2.kafka.AcknowledgeableConsumerRecord)s, each with an @:api(fs2.kafka.AcknowledgeableOffset), which we acknowledge once the record is processed. Within a poll, records can be processed in parallel.

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

Delivery is at-least-once, so processing must tolerate redelivery. A failed record can be released for another attempt, or rejected if it can never succeed.

```scala mdoc:silent
def processOrRelease(record: AcknowledgeableConsumerRecord[IO, String, String]): IO[Unit] =
  process(record.record).attempt.flatMap {
    case Right(_) => record.accept
    case Left(_)  => record.release
  }
```

Records `process` leaves without an acknowledgement, when it returns or fails, get the failure acknowledgement: they are released, or rejected with `withFailureAcknowledgement(AcknowledgeType.Reject)`. If `process` fails, `consume` fails with the same error. If it is canceled, its records are always released. As a failing `process` settles every unacknowledged record of its poll, with `Reject` handle failures per record instead. Acknowledging a record after `process` returned fails with a `RecordNotInFlightException`.

## Acknowledging

As in Java, acknowledging only records the acknowledgement in the consumer. The pending acknowledgements are sent with the next poll, once the oldest has waited for `withMaxAcknowledgementDelay`, at once while paused, and when `consume` ends. There is no commit to call.

- `record.accept`, `release`, `reject` and `renew` acknowledge one record, as does `acknowledge(AcknowledgeType)`. They are on the record and on its `offset`.

- `acknowledgeChunks` acknowledges the records of each chunk with one call per run of records of the same consumer, for example `Stream.chunk(records).map(_.offset -> AcknowledgeType.Accept).through(acknowledgeChunks[IO])` to accept a whole poll at once.

As in Java, the last acknowledgement for a record wins until it is sent. Once sent, a later acknowledgement fails with a `RecordNotInFlightException`, and so does one for a delivery the consumer no longer holds. `acknowledgeChunks` skips these.

Acknowledging doesn't ask the broker, so an acknowledgement for a record whose lock has expired is only rejected when it is sent, and the record is delivered again.

## Deserialization Failures

A record which fails to deserialize fails `consume`, and the poll's records get the failure acknowledgement. The next consumer fails on it too, until the broker archives it at the delivery count limit. Instead, we can use `attempt` deserializers, which return failures as values, and reject the failed records.

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

A record which is not acknowledged within its acquisition lock, 30 seconds by default, is delivered again, possibly to another consumer. As in Java, records are not renewed automatically: we can renew a record with `Renew`, any number of times before its final acknowledgement.

A renewal renews every unacknowledged record of the poll, without waiting for the broker. Acknowledgements made meanwhile are kept until the records are back, and no new records are polled until then. If the broker rejects a renewal, the records are delivered again.

With `withAutoRenewal(true)`, records without an acknowledgement are renewed `withRenewalMargin` before their lock ends, and at least every half of `max.poll.interval.ms`. Enable it when processing may outlast the lock.

- **Without auto-renewal, keep processing within the lock** and `max.poll.interval.ms`, as in Java.

- **With auto-renewal, a hung record** is renewed for as long as `process` runs, unless we set `withMaxProcessingTime`. Past it, the record gets the failure acknowledgement, and its later acknowledgement fails with a `RecordNotInFlightException`. The time counts from the poll, so set it well above the time to process a whole poll.

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

When something the processing needs is unavailable, polling on would acquire records only to fail them, raising their delivery count until the broker archives them. With `withPauseWhen(signal)`, the consumer polls for no new records while the signal is `true`.

```scala mdoc:silent
import fs2.concurrent.SignallingRef

// Retries until the service is back, while auto-renewal holds the record's lock.
def processRetrying(record: AcknowledgeableConsumerRecord[IO, String, String]): IO[Unit] =
  process(record.record).handleErrorWith(_ => IO.sleep(5.seconds) *> processRetrying(record))

def pausable(paused: SignallingRef[IO, Boolean]): IO[Unit] =
  KafkaShareConsumer
    .resource(settings.withAutoRenewal(true).withPauseWhen(paused))
    .use { consumer =>
      consumer.subscribeTo("jobs") *>
        consumer.consume { records =>
          records.parTraverseN(32)(record => processRetrying(record) *> record.accept).void
        }
    }
```

Deciding when to pause is up to us, for example with a circuit breaker or a health check which sets the signal. While paused:

- The poll being processed is acknowledged as usual, and its acknowledgements are sent at once. Auto-renewal keeps renewing records still held.

- With no records held, a pause longer than `max.poll.interval.ms` makes Java leave the share group until the next poll.

- If the last poll before the pause returned nothing, Java may still acquire records once. They are delivered after the pause with a delivery count one higher.

## Acknowledgement and Acquire Modes

| Property | Value | Behaviour |
| --- | --- | --- |
| `share.acknowledgement.mode` | `explicit` (default here) | Every record needs an acknowledgement before the next poll; use `consume` |
| | `implicit` (Java's default) | A poll is accepted once `process` returned; use `autoConsume` |
| `share.acquire.mode` | `record_limit` (default here) | A poll returns at most `max.poll.records` records |
| | `batch_optimized` (Java's default) | A poll may return more, to align with the broker's record batches |

### Implicit Acknowledgement

With implicit acknowledgement, records cannot be acknowledged, and `consume` fails with an `ImplicitAcknowledgementException`. Instead, `autoConsume` gives `process` plain `ConsumerRecord`s, and accepts the poll once `process` returned. If `process` fails or is canceled, the poll is not accepted, and closing the consumer releases its records. As any later poll would accept them, the consumer then refuses to consume again; close it and create a new one.

```scala mdoc:silent
val implicitMode: IO[Unit] =
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

With `batch_optimized`, a poll can exceed `max.poll.records` by up to one broker record batch, and Java may buffer records before they are polled, while their locks already run. Larger polls mean a slow record holds back more records and more renewals are needed, but they can help throughput when processing is fast and uniform.

## Several Consumers

To keep polling past slow records, we can use several consumers in the same share group, each with its own Java consumer and loop.

```scala mdoc:silent
val parallelProgram: IO[Unit] =
  KafkaShareConsumers.resource(settings, consumers = 4).use { consumers =>
    consumers.subscribeTo("jobs") *>
      consumers.consume { records =>
        records.parTraverseN(8)(processOrRelease).void
      }
  }
```

`consume` runs on every consumer until `stopConsuming`. If one of them fails, the others are canceled, which releases the records they hold, and the error is raised. With several consumers and `client.id` set, consumer `i`, counting from 0, uses `client.id-i`. `metrics` gives each consumer's metrics by its client id.

## Stopping and Shutdown

- `stopConsuming` makes `consume` return once the poll being processed is done and a final commit sent the last acknowledgements. If the broker rejects them, `consume` fails with its error, and with a `ShareCommitTimeoutException` if it does not answer within the commit timeout, which also bounds waiting for a pending renewal first. Stopping is final: consuming again fails; create a new consumer instead.

- Releasing the resource closes the Java consumer, which sends the pending acknowledgements and releases the records it still holds without an acknowledgement.

[java-share-consumer]: @KAFKA_API_BASE_URL@/org/apache/kafka/clients/consumer/KafkaShareConsumer.html
