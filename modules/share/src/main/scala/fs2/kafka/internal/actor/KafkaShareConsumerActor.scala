/*
 * Copyright 2018 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka.internal.actor

import scala.concurrent.duration.*

import cats.effect.{Async, Deferred, Ref, Resource}
import cats.effect.std.{Dispatcher, Queue}
import cats.syntax.all.*
import fs2.{Chunk, Stream}
import fs2.kafka.*
import fs2.kafka.internal.{Logging, WithShareConsumer}
import fs2.kafka.internal.actor.KafkaShareConsumerActor.*
import fs2.kafka.internal.converters.collection.*
import fs2.kafka.internal.converters.option.*
import fs2.kafka.internal.syntax.*

import org.apache.kafka.clients.consumer.AcknowledgementCommitCallback
import org.apache.kafka.common.errors.TimeoutException
import org.apache.kafka.common.TopicPartition

/**
  * Owns a Java share consumer and gives it each acknowledgement as it arrives. Java sends pending
  * acknowledgements with the next poll, and the last one for a record wins until then, so the actor
  * mostly tracks which records of the latest poll have an acknowledgement, and polls once all do.
  * Pending acknowledgements waiting longer than `maxAcknowledgementDelay` are sent with
  * `commitAsync`.
  *
  * Renewals are sent at once with `commitSync`, so that the poll which returns the renewed records
  * sends no fetch: a poll made while a renewal is pending makes Java fetch, and keep fetching until
  * it acquires records, which raises their delivery count while they wait unseen in its buffer.
  * Java lets a renewed record be acknowledged again only once a poll returned it, so an
  * acknowledgement for it waits in the actor until then, and the record is not handed out again.
  */
final private[kafka] class KafkaShareConsumerActor[F[_], K, V](
  config: ShareActorConfig[F, K, V],
  keyDeserializer: KeyDeserializer[F, K],
  valueDeserializer: ValueDeserializer[F, V],
  withConsumer: WithShareConsumer[F],
  requests: Queue[F, ShareRequest[F]],
  failedRenewals: Queue[F, (TopicPartition, Set[Long])],
  dispatcher: Dispatcher[F],
  output: Queue[F, Option[(Long, Chunk[AcknowledgeableConsumerRecord[F, K, V]])]],
  readerStarted: Ref[F, Boolean],
  shutdown: Deferred[F, Unit],
  finished: Deferred[F, KafkaShareConsumerActor.Ended]
)(implicit F: Async[F], logging: Logging[F])
    extends Acknowledger[F] {

  // After the actor finished, it holds no records.
  override def acknowledge(acks: Chunk[Acknowledgement]): F[Unit] =
    acks.head match {
      case None                           => F.unit
      case Some(_) if config.implicitMode => F.raiseError(ImplicitAcknowledgementException())
      case Some(first)                    =>
        ask[Unit](
          ShareRequest.Ack(acks, _),
          RecordNotInFlightException(first.offset._1, first.offset._2)
        )
    }

  def stopConsuming: F[Unit] = finished
    .tryGet
    .flatMap(_.fold(requests.offer(ShareRequest.Stop()))(_ => F.unit))

  def records: Stream[F, AcknowledgeableConsumerRecord[F, K, V]] = polls.flatten

  // The polls, each a stream which hands it out, so merged polls are handed out after the merge.
  def polls: Stream[F, Stream[F, AcknowledgeableConsumerRecord[F, K, V]]] =
    Stream
      .eval(readerStarted.getAndSet(true))
      .flatMap {
        case true  => Stream.raiseError[F](StreamAlreadyStartedException())
        case false =>
          // As in core, closing the consumer under the stream ends it normally.
          val failed = finished
            .get
            .flatMap {
              case Ended.Failed(error)        => F.pure(error.asLeft[Unit])
              case Ended.Drained(Left(error)) => F.pure(error.asLeft[Unit])
              case Ended.Shutdown             => F.pure(().asRight[Throwable])
              // The output ends the stream.
              case Ended.Drained(Right(_)) => F.never[Either[Throwable, Unit]]
            }
          // The finalizer also covers an interruption right after Start.
          (Stream.exec(requests.offer(ShareRequest.Start())) ++
            Stream
              .fromQueueNoneTerminated(output)
              .map { case (id, chunk) =>
                handOut(chunk, requests.offer(ShareRequest.HandedOut(id)))
              }).onFinalize(requests.offer(ShareRequest.ReaderClosed())).interruptWhen(failed)
      }

  // A poll counts as handed out as the stream takes its chunk. In implicit mode, where that
  // accepts it, only once the stream asks for more, so records a `take` skips are not accepted.
  private def handOut(
    chunk: Chunk[AcknowledgeableConsumerRecord[F, K, V]],
    handedOut: F[Unit]
  ): Stream[F, AcknowledgeableConsumerRecord[F, K, V]] =
    if (config.implicitMode) Stream.chunk(chunk) ++ Stream.exec(handedOut)
    else Stream.exec(handedOut) ++ Stream.chunk(chunk)

  // As in core, closing the consumer ends it normally.
  def awaitTermination: F[Unit] =
    finished
      .get
      .flatMap {
        case Ended.Failed(error)        => F.raiseError(error)
        case Ended.Drained(Left(error)) => F.raiseError(error)
        case _                          => F.unit
      }

  // As in core, this does not wait for the actor to end.
  def terminate: F[Unit] = shutdown.complete(()).void

  // Sends a request and waits for its answer. Once the actor ended, the answer is `afterDrain` if
  // it drained, or else the reason it ended; once shut down, the consumer is.
  private def ask[A](
    request: (Either[Throwable, A] => F[Unit]) => ShareRequest[F],
    afterDrain: => Throwable
  ): F[A] = {
    def afterEnd(ended: Ended): F[A] =
      shutdown
        .tryGet
        .flatMap {
          case Some(_) => F.raiseError[A](ConsumerShutdownException())
          case None    =>
            ended match {
              case Ended.Drained(_)    => F.raiseError[A](afterDrain)
              case Ended.Failed(error) => F.raiseError[A](error)
              case Ended.Shutdown      => F.raiseError[A](ConsumerShutdownException())
            }
        }
    // Nothing takes requests once the actor ended.
    finished
      .tryGet
      .flatMap {
        case Some(ended) => afterEnd(ended)
        case None        =>
          Deferred[F, Either[Throwable, A]].flatMap { done =>
            requests.offer(request(done.complete(_).void)) *>
              F.race(finished.get, done.get)
                .flatMap {
                  case Right(result) => result.liftTo[F]
                  case Left(ended)   =>
                    done.tryGet.flatMap(_.fold(afterEnd(ended))(_.liftTo[F]))
                }
          }
      }
  }

  // --- Renewal results ---

  // Java reports results here during consumer calls. Only failed renewals matter: their records
  // lost their lock.
  private val callback: AcknowledgementCommitCallback = (offsets, error) =>
    Option(error).foreach(_ =>
      dispatcher.unsafeRunSync(
        offsets
          .asScala
          .toList
          .traverse_ { case (partition, values) =>
            failedRenewals.offer(partition.topicPartition -> values.asScala.map(_.longValue).toSet)
          }
      )
    )

  // A renewal never reported is given up after its lock and Kafka's API timeout.
  private def withoutUnreported(state: ShareState[F], now: FiniteDuration): ShareState[F] = {
    val limit = state.lock.fold(config.apiTimeout)(_ + config.apiTimeout)
    state.withoutRenewing(state.renewing.filter(p => state.held(p).lockedSince + limit <= now))
  }

  private def dropFailedRenewals(state: ShareState[F]): F[ShareState[F]] =
    failedRenewals
      .tryTakeN(None)
      .map(_.foldLeft(state) { case (s, (partition, offsets)) =>
        s.withoutRenewing(offsets.map(partition -> _))
      })

  // A waiting acknowledgement whose record lost its lock before a poll returned it fails.
  private def settleLost(state: ShareState[F]): F[ShareState[F]] =
    if (state.waiters.isEmpty) F.pure(state)
    else {
      val (settled, effects) = state.settledLost
      effects.sequence_.as(settled)
    }

  // --- Requests ---

  private def receive(state: ShareState[F], request: ShareRequest[F]): F[ShareState[F]] =
    request match {
      case ShareRequest.Start() =>
        withConsumer
          .blocking(_.subscription().isEmpty)
          .ifM(F.raiseError(NotSubscribedException()), F.pure(state.copy(started = true)))
      case ShareRequest.Stop()        => F.pure(state.copy(stopping = true))
      case ShareRequest.HandedOut(id) =>
        F.monotonic
          .map(state.withHandedOut(id, _))
          .map(s =>
            if (config.implicitMode && id == state.pollId) s.copy(acceptPending = true) else s
          )
      case ShareRequest.Paused(paused)  => F.pure(state.copy(paused = paused))
      case ShareRequest.ReaderClosed()  => releaseUnpulled(state.copy(readerClosed = true))
      case ShareRequest.Ack(acks, done) =>
        ackAll(state, Vector(acks -> done), send = false, byUser = true)
    }

  // Releases a poll the stream never took; renewing records once a poll returns them. In implicit
  // mode, Java can't release, so the poll stays unpulled and closing releases it.
  private def releaseUnpulled(state: ShareState[F]): F[ShareState[F]] =
    if (config.implicitMode) F.pure(state)
    else {
      val unacked  = state.unacked.map(_._1).toSet
      val renewing = state.unpulled.filter(state.renewing)
      release(
        state.copy(
          unpulled = Set.empty,
          deferred = state.deferred ++ renewing.map(_ -> AcknowledgeType.Release)
        ),
        state.unpulled.filter(unacked).toVector
      )
    }

  private def release(
    state: ShareState[F],
    offsets: Vector[TopicPartitionOffset]
  ): F[ShareState[F]] =
    if (offsets.isEmpty) F.pure(state)
    else ackOwn(state, offsets.map(ack(state, _, AcknowledgeType.Release)), byUser = false)

  private val noCallback: Either[Throwable, Unit] => F[Unit] = _ => F.unit

  private def ack(
    state: ShareState[F],
    offset: TopicPartitionOffset,
    kind: AcknowledgeType
  ): Acknowledgement =
    Acknowledgement(offset, state.held(offset).deliveryCount, kind)

  // Acknowledges and sends the actor's own acknowledgements; a failure fails the actor. `byUser` renewals
  // restart the time held.
  private def ackOwn(
    state: ShareState[F],
    acks: Vector[Acknowledgement],
    byUser: Boolean
  ): F[ShareState[F]] =
    ackAll(
      state,
      Vector(Chunk.from(acks) -> ((result: Either[Throwable, Unit]) => result.liftTo[F])),
      send = true,
      byUser
    )

  // Gives several requests' acknowledgements to Java in one call. Acknowledgements for renewing records wait for
  // the poll which returns them; renewals, or everything if `send` is set, are sent at once.
  private def ackAll(
    state: ShareState[F],
    batch: Vector[(Chunk[Acknowledgement], Either[Throwable, Unit] => F[Unit])],
    send: Boolean,
    byUser: Boolean
  ): F[ShareState[F]] =
    ackResults(state, batch, send, byUser).flatMap { case (acked, outcomes) =>
      // Completes once a poll returns the renewing records and Java recorded their acknowledgements.
      val waiters = outcomes.collect {
        case (request, Right(_)) if request.waiting.nonEmpty =>
          Waiter(request.waiting.map(a => a.offset -> a.deliveryCount).toSet, request.done)
      }
      outcomes
        .traverse_ {
          case (request, Right(_)) if request.waiting.nonEmpty => F.unit
          case (request, result)                               => request.done(result)
        }
        .as(acked.copy(waiters = acked.waiters ++ waiters))
    }

  // Gives the requests' acknowledgements to Java, with each request's result; once Java failed, it completes
  // every request with the error and fails.
  private def ackResults(
    state: ShareState[F],
    batch: Vector[(Chunk[Acknowledgement], Either[Throwable, Unit] => F[Unit])],
    send: Boolean,
    byUser: Boolean
  ): F[(ShareState[F], Vector[(Acking[F], Either[Throwable, Unit])])] = {
    val ackings = batch.map { case (own, done) => Acking(state, own, done) }
    val now     = ackings.flatMap(_.now)
    val renewal = now.exists(_.acknowledgeType == AcknowledgeType.Renew)
    val sending = send || renewal
    // Per request: the acknowledgements Java recorded, those it no longer holds, and its error.
    val calls: F[Vector[(Vector[Acknowledgement], Vector[Acknowledgement], Option[Throwable])]] =
      if (now.isEmpty && !send) F.pure(ackings.as((Vector.empty, Vector.empty, None)))
      else
        withConsumer.blocking { consumer =>
          // Stops at a request's first error, keeping what Java recorded before it.
          val recorded = ackings.map { request =>
            request
              .now
              .foldLeft(
                (
                  Vector.empty[Acknowledgement],
                  Vector.empty[Acknowledgement],
                  Option.empty[Throwable]
                )
              ) {
                case (stop @ (_, _, Some(_)), _) => stop
                case ((done, gone, None), a)     =>
                  Either.catchNonFatal(
                    consumer.acknowledge(state.held(a.offset).record, a.acknowledgeType.asJava)
                  ) match {
                    case Right(_) => (done :+ a, gone, None)
                    // Java no longer holds it, e.g. after a commit failed once sent. Its other
                    // reasons, a closed consumer and implicit mode, cannot happen here.
                    case Left(_: IllegalStateException) => (done, gone :+ a, None)
                    case Left(error)                    => (done, gone, Some(error))
                  }
              }
          }
          // A failed send fails the actor. A renewal waits for the broker, up to the renewal margin; one
          // still pending after it is returned by a poll which also fetches, as with `commitAsync`.
          if (renewal)
            Either
              .catchOnly[TimeoutException](
                consumer.commitSync(config.settings.renewalMargin.toJava)
              )
              .void
              .getOrElse(())
          else if (sending) consumer.commitAsync()
          recorded
        }
    (F.monotonic, calls.attempt)
      .tupled
      .flatMap {
        case (at, Right(results)) =>
          // Tracks what Java recorded, also of a request that failed part-way. The last one wins.
          // Records Java no longer holds count as acknowledged, so that they are dropped once sent.
          val outcomes = ackings.zip(results)
          val last     = outcomes
            .flatMap { case (_, (done, _, _)) => done }
            .map(a => a.offset -> a.acknowledgeType)
            .toMap
          val gone     = outcomes.flatMap { case (_, (_, gone, _)) => gone.map(_.offset) }.toSet
          val waiting  = outcomes.collect { case (request, (_, _, None)) => request.waiting }.flatten
          val withLast = state.withAcked(last, waiting, at, byUser)
          val acked    = withLast.copy(acked = withLast.acked ++ gone)
          val answers  = outcomes.map { case (request, (_, gone, error)) =>
            request -> error
              .orElse(
                (gone ++ request.notHeld)
                  .headOption
                  .map(a => RecordNotInFlightException(a.offset._1, a.offset._2))
              )
              .toLeft(())
          }
          F.pure((if (sending) acked.withoutAcked else acked) -> answers)
        case (_, Left(error)) =>
          batch.traverse_(_._2(Left(error))) *>
            F.raiseError[(ShareState[F], Vector[(Acking[F], Either[Throwable, Unit])])](error)
      }
  }

  // In implicit mode, a commit would accept a poll the stream has not taken.
  private def holdsUntakenPoll(state: ShareState[F]): Boolean =
    config.implicitMode && state.unpulled.nonEmpty

  // Java's `commitSync`, which sends every pending acknowledgement and waits for the broker. Results per
  // partition are not reported, as with the other sends.
  private def commitSync(state: ShareState[F], timeout: FiniteDuration): F[Unit] =
    if (holdsUntakenPoll(state)) F.unit
    else
      withConsumer
        .blocking(consumer => consumer.commitSync(timeout.toJava): Unit)
        .adaptError { case error: TimeoutException =>
          ShareCommitTimeoutException(timeout, error)
        }

  // --- Polling ---

  private def poll(state: ShareState[F]): F[ShareState[F]] =
    F.monotonic
      .flatMap(start =>
        withConsumer
          .blocking { consumer =>
            if (consumer.subscription().isEmpty) None
            else
              Some(
                consumer.poll(config.settings.pollTimeout.toJava).iterator().asScala.toVector ->
                  ShareTimeouts.acquisitionLock(consumer)
              )
          }
          .flatMap {
            // Unsubscribed: no fetch was sent, and requests are taken while waiting. Leaving the
            // group released the records, so pending renewals will never be returned.
            case None =>
              val idle = state.withoutRenewing(state.renewing)
              takeWithin(subscriptionCheckInterval).flatMap(_.fold(F.pure(idle))(receive(idle, _)))
            case Some((records, reported)) =>
              val lock = reported.orElse(state.lock)
              // Failed renewals are applied first, so their redelivery here is a new delivery.
              (F.monotonic, dropFailedRenewals(state))
                .tupled
                .flatMap { case (now, current) =>
                  polled(current, records, lock, start, now).map(withoutUnreported(_, now))
                }
          }
      )

  // Returned renewed records get their waiting acknowledgements; new records are handed out, or released
  // if not admitted. New records count as acquired at the poll's start; records Java buffered from
  // an earlier fetch are older, which the renewal margin covers.
  private def polled(
    state: ShareState[F],
    records: Vector[KafkaByteConsumerRecord],
    lock: Option[FiniteDuration],
    acquiredAt: FiniteDuration,
    now: FiniteDuration
  ): F[ShareState[F]] = {
    val withOffsets       = records.map(r => offsetOf(r) -> r)
    val (returned, fresh) = withOffsets.partition { case (p, r) =>
      state.renewing(p) && state.holds(p, deliveryCountOf(r))
    }
    val admitting = state.admitting
    for {
      admitted <-
        if (admitting) fresh.traverse { case (p, r) => admit(p, r, acquiredAt, now) }
        else F.pure(Vector.empty)
      // Held only to be released below.
      rejected =
        if (admitting) Vector.empty else fresh.map { case (p, r) => p -> held(r, acquiredAt, now) }
      next = state.polled(
               // A renewed record's lock runs from when its renewal was sent.
               returned.map { case (p, r) => p -> state.held(p).copy(record = r) },
               admitted.map(_._1),
               rejected,
               lock,
               holdAdmitted = !config.implicitMode
             )
      _ <- logging
             .log(ShareLogEntry.Polled(records.size, returned.size, next.held.size))
             .whenA(records.nonEmpty)
      _ <-
        output.offer(Some(next.pollId -> Chunk.from(admitted.map(_._2)))).whenA(admitted.nonEmpty)
      // Waiting acknowledgements are the user's, sent at once rather than behind the slowest record.
      waiting = returned.flatMap { case (p, r) =>
                  next.deferred.get(p).map(kind => (p, deliveryCountOf(r), kind))
                }
      // One request per waiting acknowledgement, to settle its waiters with its own result.
      waitingBatch = waiting.map { case (p, _, kind) =>
                       Chunk.singleton(ack(next, p, kind)) -> noCallback
                     }
      releases = rejected.map { case (p, _) => ack(next, p, AcknowledgeType.Release) }
      batch    = waitingBatch ++
                (if (releases.isEmpty) Vector.empty else Vector(Chunk.from(releases) -> noCallback))
      acked <-
        if (batch.isEmpty) F.pure(next)
        else
          ackResults(
            next.copy(deferred = next.deferred -- waiting.map(_._1)),
            batch,
            send = true,
            byUser = true
          ).flatMap { case (s, results) =>
            val (waitingResults, releaseResults) = results.map(_._2).splitAt(waiting.size)
            val (settled, effects)               =
              waiting
                .zip(waitingResults)
                .foldLeft((s, Vector.empty[F[Unit]])) {
                  case ((current, done), ((p, count, _), result)) =>
                    val (next, more) = current.settled(p, count, result)
                    (next, done ++ more)
                }
            // A failed release fails the actor.
            effects.sequence_ *> releaseResults.traverse_(_.liftTo[F]).as(settled)
          }
    } yield acked
  }

  private def held(
    record: KafkaByteConsumerRecord,
    acquiredAt: FiniteDuration,
    now: FiniteDuration
  ) =
    Held(deliveryCountOf(record), record, acquiredAt, now)

  private def admit(
    offset: TopicPartitionOffset,
    record: KafkaByteConsumerRecord,
    acquiredAt: FiniteDuration,
    now: FiniteDuration
  ): F[((TopicPartitionOffset, Held), AcknowledgeableConsumerRecord[F, K, V])] = {
    val delivery = held(record, acquiredAt, now)
    ConsumerRecord
      .fromJava(record, keyDeserializer, valueDeserializer)
      .map(decoded =>
        (offset -> delivery) ->
          AcknowledgeableConsumerRecord(
            decoded,
            AcknowledgeableOffset(offset._1, offset._2, delivery.deliveryCount, this)
          )
      )
  }

  // --- Automatic renewal ---

  // Unacknowledged records are renewed at once while an acknowledgement waits for a renewed record, so that the
  // consumer can poll it, and with automatic renewal at the renewal interval.
  private def nextRenewal(state: ShareState[F]): Option[FiniteDuration] = {
    // Only scans the records when something is to be renewed.
    lazy val unacked = state.unacked.map(_._2.lockedSince)
    if (state.allSettled || (state.deferred.isEmpty && !config.settings.autoRenewal)) None
    else if (state.deferred.nonEmpty) Some(unacked.min)
    // Also after the stream ended, as records may still be processed downstream.
    else if (config.settings.autoRenewal)
      Some(unacked.min + state.lock.fold(unknownLockRenewalInterval)(renewalInterval))
    else None
  }

  // The renewal margin before the lock ends, or half the lock, early enough for
  // `max.poll.interval.ms`.
  private def renewalInterval(lock: FiniteDuration): FiniteDuration = {
    val byMargin =
      if (config.settings.renewalMargin < lock) lock - config.settings.renewalMargin else lock / 2
    byMargin.min(config.maxPollInterval / 2)
  }

  private def renew(state: ShareState[F]): F[ShareState[F]] =
    ackOwn(
      state,
      state.unacked.map { case (p, _) => ack(state, p, AcknowledgeType.Renew) },
      byUser = false
    )

  // --- Max acknowledgement delay ---

  // When the pending acknowledgements are sent without waiting for the next poll: at once while paused, as
  // no poll comes, also to accept a taken poll in implicit mode.
  private def nextSend(state: ShareState[F]): Option[FiniteDuration] =
    if (state.paused && state.renewing.isEmpty)
      state
        .unsentSince
        .orElse(if (state.acceptPending && !holdsUntakenPoll(state)) Some(Duration.Zero) else None)
    else (state.unsentSince, config.settings.maxAcknowledgementDelay).mapN(_ + _)

  // Java's `commitAsync`, which sends every pending acknowledgement.
  private def commitAsync(state: ShareState[F]): F[ShareState[F]] =
    if (holdsUntakenPoll(state)) F.pure(state)
    else withConsumer.blocking(_.commitAsync()).as(state.withoutAcked)

  // --- Max processing time ---

  // When the max processing time releases each unacknowledged record the stream took, counted from
  // when it took it or the user's latest renewal.
  private def limits(state: ShareState[F]): Vector[(TopicPartitionOffset, FiniteDuration)] =
    config
      .settings
      .maxProcessingTime
      .fold(Vector.empty[(TopicPartitionOffset, FiniteDuration)])(max =>
        state.unacked.collect { case (p, h) if !state.unpulled(p) => p -> (h.heldSince + max) }
      )

  // Releases overdue records, so they are delivered again and the consumer can poll.
  private def releaseOverdue(
    state: ShareState[F],
    records: Vector[TopicPartitionOffset]
  ): F[ShareState[F]] =
    config
      .settings
      .maxProcessingTime
      .traverse_(max => logging.log(ShareLogEntry.ReleasingOverdue(records, max))) *>
      release(state, records)

  // --- Running ---

  // Takes a request, or `None` after the duration, without losing one taken as the timer fires.
  private def takeWithin(duration: FiniteDuration): F[Option[ShareRequest[F]]] =
    F.racePair(requests.take, F.sleep(duration))
      .flatMap {
        case Left((taken, timer)) => timer.cancel *> taken.embedNever.map(Some(_))
        case Right((taker, _))    =>
          taker.cancel *> taker.join.flatMap(_.fold(F.pure(None), F.raiseError, _.map(Some(_))))
      }

  // Receives requests in order, giving consecutive acknowledgements to Java in one call.
  private def receiveAll(state: ShareState[F], taken: List[ShareRequest[F]]): F[ShareState[F]] =
    taken match {
      case Nil                           => F.pure(state)
      case (_: ShareRequest.Ack[F]) :: _ =>
        val (acks, rest) = taken.span(_.isInstanceOf[ShareRequest.Ack[F]])
        val batch        = acks.collect { case ShareRequest.Ack(a, done) => a -> done }.toVector
        ackAll(state, batch, send = false, byUser = true).flatMap(receiveAll(_, rest))
      case request :: rest => receive(state, request).flatMap(receiveAll(_, rest))
    }

  // Ends with the result of the final commit after stopping.
  private def loop(state: ShareState[F]): F[Either[Throwable, Unit]] =
    requests
      .tryTakeN(None)
      .flatMap(receiveAll(state, _))
      .flatMap(dropFailedRenewals)
      .flatMap(settleLost)
      .flatMap { current =>
        F.monotonic
          .flatMap { now =>
            val renewal = nextRenewal(current)
            val limit   = limits(current)
            val overdue = limit.collect { case (p, at) if at <= now => p }
            val send    = nextSend(current)
            val wakeAt  = (renewal.toList ++ send ++ limit.map(_._2)).reduceOption(_ min _)
            if (current.isFinished)
              commitSync(current, config.settings.commitTimeout).attempt
            else if (overdue.nonEmpty) releaseOverdue(current, overdue).flatMap(loop)
            // Stopping after the stream ended: nothing will acknowledge the rest.
            else if (current.stopping && current.readerClosed && current.unacked.nonEmpty)
              release(current, current.unacked.map(_._1)).flatMap(loop)
            else if (renewal.exists(_ <= now)) renew(current).flatMap(loop)
            // Before polling, which may wait while unsubscribed.
            else if (send.exists(_ <= now)) commitAsync(current).flatMap(loop)
            else if (current.canPoll) poll(current).flatMap(loop)
            else
              wakeAt
                .fold(requests.take.map(Option(_)))(at => takeWithin(at - now))
                .flatMap(_.fold(F.pure(current))(receive(current, _)))
                .flatMap(loop)
          }
      }

  // The loop, told of the pause signal's changes. It starts with the signal's value, so that it
  // does not poll before the first change arrives.
  private def withPauses: F[Either[Throwable, Unit]] =
    config
      .settings
      .pauseWhen
      .fold(loop(ShareState[F]())) { signal =>
        val changes =
          signal.discrete.evalMap(p => requests.offer(ShareRequest.Paused(p))).compile.drain
        signal
          .get
          .flatMap(paused => F.race(changes *> F.never[Unit], loop(ShareState[F](paused = paused))))
          .flatMap {
            case Right(result) => F.pure(result)
            case Left(_)       => F.never[Either[Throwable, Unit]]
          }
      }

  // Terminating ends without draining, so later requests fail.
  private[actor] def run: F[Unit] =
    F.race(
        shutdown.get,
        withConsumer.blocking(_.setAcknowledgementCommitCallback(callback)) *> withPauses
      )
      .attempt
      .flatMap {
        // A failed final commit fails the stream.
        case Right(Right(Left(error))) => finished.complete(Ended.Drained(Left(error))).void
        case Right(Right(last))        =>
          output.offer(None) *> finished.complete(Ended.Drained(last)).void
        case Right(Left(_)) => output.offer(None) *> finished.complete(Ended.Shutdown).void
        case Left(error)    => finished.complete(Ended.Failed(error)).void
      }

}

private[kafka] object KafkaShareConsumerActor {

  // How the actor ended.
  sealed abstract class Ended

  object Ended {

    // After stopping, with the result of the final commit.
    final case class Drained(lastCommit: Either[Throwable, Unit]) extends Ended

    // Terminated, or closed before it drained.
    case object Shutdown extends Ended

    final case class Failed(error: Throwable) extends Ended

  }

  def offsetOf(record: KafkaByteConsumerRecord): TopicPartitionOffset =
    new TopicPartition(record.topic, record.partition) -> record.offset

  def deliveryCountOf(record: KafkaByteConsumerRecord): Short =
    record.deliveryCount().toScala.fold(1: Short)(_.shortValue)

  // One request's held acknowledgements, to give Java now or waiting for a renewed record, and its first
  // acknowledgement for a delivery not held.
  final case class Acking[F[_]](
    done: Either[Throwable, Unit] => F[Unit],
    now: Vector[Acknowledgement],
    waiting: Vector[Acknowledgement],
    notHeld: Option[Acknowledgement]
  )

  object Acking {

    def apply[F[_]](
      state: ShareState[F],
      acks: Chunk[Acknowledgement],
      done: Either[Throwable, Unit] => F[Unit]
    ): Acking[F] = {
      val (held, notHeld) = acks.toVector.partition(a => state.holds(a.offset, a.deliveryCount))
      val (waiting, now)  = held.partition(a => state.renewing(a.offset))
      Acking(done, now, waiting, notHeld.headOption)
    }

  }

  // How often an unsubscribed consumer checks for a subscription.
  val subscriptionCheckInterval: FiniteDuration = 50.millis

  // Half of the shortest lock a broker allows, while the broker has not reported the lock.
  val unknownLockRenewalInterval: FiniteDuration = 500.millis

  def resource[F[_], K, V](
    settings: ShareConsumerSettings[F, K, V],
    keyDeserializer: KeyDeserializer[F, K],
    valueDeserializer: ValueDeserializer[F, V],
    withConsumer: WithShareConsumer[F],
    dispatcher: Dispatcher[F],
    id: Int
  )(implicit F: Async[F]): Resource[F, KafkaShareConsumerActor[F, K, V]] =
    for {
      logging        <- Resource.eval(Logging.create[F]("fs2.kafka.KafkaShareConsumer$" + id))
      requests       <- Resource.eval(Queue.unbounded[F, ShareRequest[F]])
      failedRenewals <- Resource.eval(Queue.unbounded[F, (TopicPartition, Set[Long])])
      output         <-
        Resource.eval(
          Queue.unbounded[F, Option[(Long, Chunk[AcknowledgeableConsumerRecord[F, K, V]])]]
        )
      readerStarted <- Resource.eval(Ref.of[F, Boolean](false))
      shutdown      <- Resource.eval(Deferred[F, Unit])
      finished      <- Resource.eval(Deferred[F, Ended])
      config        <- Resource.eval(ShareActorConfig.from(settings))
      actor          = new KafkaShareConsumerActor(
                config,
                keyDeserializer,
                valueDeserializer,
                withConsumer,
                requests,
                failedRenewals,
                dispatcher,
                output,
                readerStarted,
                shutdown,
                finished
              )(F, logging)
      // Once closed, requests fail with ConsumerShutdownException.
      _ <- Resource.make(F.start(actor.run))(fiber =>
             fiber.cancel *> shutdown.complete(()) *> finished.complete(Ended.Shutdown).void
           )
    } yield actor

}
