/*
 * Copyright 2018 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka.internal.actor

import java.util.concurrent.ConcurrentLinkedQueue

import scala.concurrent.duration.*

import cats.effect.{Async, Deferred, Ref, Resource}
import cats.effect.std.Queue
import cats.syntax.all.*
import fs2.{Chunk, Stream}
import fs2.kafka.*
import fs2.kafka.internal.{Logging, WithShareConsumer}
import fs2.kafka.internal.actor.KafkaShareConsumerActor.*
import fs2.kafka.internal.converters.collection.*
import fs2.kafka.internal.syntax.*

import org.apache.kafka.clients.consumer.AcknowledgementCommitCallback
import org.apache.kafka.common.{KafkaException, TopicIdPartition, TopicPartition}
import org.apache.kafka.common.errors.TimeoutException

/**
  * Owns a Java share consumer and stages each decision in it as it arrives. Java sends staged
  * decisions with the next poll or commit, and the last one for a record wins until then, so the
  * actor mostly tracks which records of the latest poll have a decision, and polls once all do.
  * Staged decisions waiting longer than `maxAcknowledgementDelay` are sent with `commitAsync`.
  *
  * Renewals are sent at once. Java lets a renewed record be decided again only once a poll returned
  * it, so a decision for it waits in the actor until then, and the record is not handed out again.
  */
final private[kafka] class KafkaShareConsumerActor[F[_], K, V](
  config: ShareActorConfig[F, K, V],
  keyDeserializer: KeyDeserializer[F, K],
  valueDeserializer: ValueDeserializer[F, V],
  withConsumer: WithShareConsumer[F],
  requests: Queue[F, ShareRequest[F]],
  output: Queue[F, Option[(Long, Chunk[AcknowledgeableConsumerRecord[F, K, V]])]],
  readerStarted: Ref[F, Boolean],
  shutdown: Deferred[F, Unit],
  finished: Deferred[F, KafkaShareConsumerActor.Ended]
)(implicit F: Async[F], logging: Logging[F])
    extends Stager[F] {

  // After the actor finished, it holds no records.
  override def stage(decisions: Chunk[Decision]): F[Unit] =
    if (decisions.isEmpty) F.unit
    else if (config.implicitMode) F.raiseError(ImplicitAcknowledgementException())
    else
      ask[Unit](
        ShareRequest.Stage(decisions, _),
        _ =>
          Left(
            RecordNotInFlightException(decisions.head.get.offset._1, decisions.head.get.offset._2)
          )
      )

  // Java's `commitSync`; after the actor finished, the result of its final commit.
  def commitSync(timeout: FiniteDuration): F[CommitResult] =
    if (timeout <= Duration.Zero)
      F.raiseError(new IllegalArgumentException("commit timeout must be positive"))
    else ask(ShareRequest.CommitSync(timeout, _), identity)

  // Java's `commitAsync`; after the actor finished, fails only if its final commit failed.
  def commitAsync: F[Unit] =
    ask(ShareRequest.CommitAsync(_), _.void)

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

  // Sends a request and waits for its answer. Once the actor ended, the answer comes from
  // `afterDrain` if it drained, or else it is the reason it ended; once shut down, the consumer
  // is.
  private def ask[A](
    request: (Either[Throwable, A] => F[Unit]) => ShareRequest[F],
    afterDrain: Either[Throwable, CommitResult] => Either[Throwable, A]
  ): F[A] = {
    def afterEnd(ended: Ended): F[A] =
      shutdown
        .tryGet
        .flatMap {
          case Some(_) => F.raiseError[A](ConsumerShutdownException())
          case None    =>
            ended match {
              case Ended.Drained(last) => afterDrain(last).liftTo[F]
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

  private val failedRenewals = new ConcurrentLinkedQueue[(TopicPartition, Set[Long])]()

  // Java reports results here during consumer calls. Only failed renewals matter: their records
  // lost their lock.
  private val callback: AcknowledgementCommitCallback = (offsets, error) =>
    if (error != null)
      offsets
        .asScala
        .foreach { case (partition, values) =>
          failedRenewals.add(partition.topicPartition -> values.asScala.map(_.longValue).toSet)
        }

  // A renewal never reported is given up after its lock and Kafka's API timeout.
  private def withoutUnreported(state: ShareState[F], now: FiniteDuration): ShareState[F] = {
    val limit = state.lock.fold(config.apiTimeout)(_ + config.apiTimeout)
    state.withoutRenewing(state.renewing.filter(p => state.held(p).lockedSince + limit <= now))
  }

  private def dropFailedRenewals(state: ShareState[F]): F[ShareState[F]] =
    F.delay {
      if (failedRenewals.isEmpty) state
      else
        Iterator
          .continually(failedRenewals.poll())
          .takeWhile(_ != null)
          .foldLeft(state) { case (s, (partition, offsets)) =>
            s.withoutRenewing(offsets.map(partition -> _))
          }
    }

  // A waiting decision whose record lost its lock before a poll returned it fails.
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
      case ShareRequest.Paused(paused)         => F.pure(state.copy(paused = paused))
      case ShareRequest.ReaderClosed()         => releaseUnpulled(state.copy(readerClosed = true))
      case ShareRequest.Stage(decisions, done) =>
        stageAll(state, Vector(decisions -> done), send = false, byUser = true)
      case ShareRequest.CommitSync(timeout, done) => commit(state, timeout, done)
      // A failed send fails the actor, as Java's `commitAsync` fails only on a broken consumer.
      case ShareRequest.CommitAsync(done) =>
        sendStaged(state).attempt.flatTap(r => done(r.void)).flatMap(_.liftTo[F])
    }

  // Releases a poll the stream never took; renewing records once a poll returns them. In implicit
  // mode, Java can't release, so the poll stays unpulled and closing releases it.
  private def releaseUnpulled(state: ShareState[F]): F[ShareState[F]] =
    if (config.implicitMode) F.pure(state)
    else {
      val undecided = state.undecided.map(_._1).toSet
      val renewing  = state.unpulled.filter(state.renewing)
      release(
        state.copy(
          unpulled = Set.empty,
          deferred = state.deferred ++ renewing.map(_ -> AcknowledgeType.Release)
        ),
        state.unpulled.filter(undecided).toVector
      )
    }

  private def release(
    state: ShareState[F],
    offsets: Vector[TopicPartitionOffset]
  ): F[ShareState[F]] =
    if (offsets.isEmpty) F.pure(state)
    else stageOwn(state, offsets.map(decision(state, _, AcknowledgeType.Release)), byUser = false)

  private val noCallback: Either[Throwable, Unit] => F[Unit] = _ => F.unit

  private def decision(
    state: ShareState[F],
    offset: TopicPartitionOffset,
    kind: AcknowledgeType
  ): Decision =
    Decision(offset, state.held(offset).deliveryCount, kind)

  // Stages and sends the actor's own decisions; a failure fails the actor. `byUser` renewals
  // restart the time held.
  private def stageOwn(
    state: ShareState[F],
    decisions: Vector[Decision],
    byUser: Boolean
  ): F[ShareState[F]] =
    stageAll(
      state,
      Vector(Chunk.from(decisions) -> ((result: Either[Throwable, Unit]) => result.liftTo[F])),
      send = true,
      byUser
    )

  // Stages several requests' decisions in one call to Java. Decisions for renewing records wait for
  // the poll which returns them; renewals, or everything if `send` is set, are sent at once.
  private def stageAll(
    state: ShareState[F],
    batch: Vector[(Chunk[Decision], Either[Throwable, Unit] => F[Unit])],
    send: Boolean,
    byUser: Boolean
  ): F[ShareState[F]] =
    stageResults(state, batch, send, byUser).flatMap { case (staged, outcomes) =>
      // Completes once a poll returns the renewing records and Java staged their decisions.
      val waiters = outcomes.collect {
        case (request, Right(_)) if request.waiting.nonEmpty =>
          Waiter(request.waiting.map(_.offset).toSet, request.done)
      }
      outcomes
        .traverse_ {
          case (request, Right(_)) if request.waiting.nonEmpty => F.unit
          case (request, result)                               => request.done(result)
        }
        .as(staged.copy(waiters = staged.waiters ++ waiters))
    }

  // Stages the requests' decisions, giving each request's result; once Java failed, it completes
  // every request with the error and fails.
  private def stageResults(
    state: ShareState[F],
    batch: Vector[(Chunk[Decision], Either[Throwable, Unit] => F[Unit])],
    send: Boolean,
    byUser: Boolean
  ): F[(ShareState[F], Vector[(Staging[F], Either[Throwable, Unit])])] = {
    val stagings = batch.map { case (own, done) => Staging(state, own, done) }
    val now      = stagings.flatMap(_.now)
    val sending  = send || now.exists(_.acknowledgeType == AcknowledgeType.Renew)
    // Per request: the decisions Java staged, those it no longer holds, and its error.
    val calls: F[Vector[(Vector[Decision], Vector[Decision], Option[Throwable])]] =
      if (now.isEmpty && !send) F.pure(stagings.as((Vector.empty, Vector.empty, None)))
      else
        withConsumer.blocking { consumer =>
          val staged = stagings.map { request =>
            val done  = Vector.newBuilder[Decision]
            val gone  = Vector.newBuilder[Decision]
            val error = scala
              .util
              .Try(
                request
                  .now
                  .foreach { d =>
                    try {
                      consumer.acknowledge(state.held(d.offset).record, d.acknowledgeType.asJava)
                      done += d
                    } catch {
                      // Java no longer holds it, e.g. after a commit failed once sent. Its other
                      // reasons, a closed consumer and implicit mode, cannot happen here.
                      case _: IllegalStateException => gone += d
                    }
                  }
              )
              .failed
              .toOption
            (done.result(), gone.result(), error)
          }
          // A failed send fails the actor.
          if (sending) consumer.commitAsync()
          staged
        }
    (F.monotonic, calls.attempt)
      .tupled
      .flatMap {
        case (at, Right(results)) =>
          // Tracks what Java staged, also of a request that failed part-way. The last one wins.
          // Records Java no longer holds count as decided, so that they are dropped once sent.
          val outcomes = stagings.zip(results)
          val last     = outcomes
            .flatMap { case (_, (done, _, _)) => done }
            .map(d => d.offset -> d.acknowledgeType)
            .toMap
          val gone     = outcomes.flatMap { case (_, (_, gone, _)) => gone.map(_.offset) }.toSet
          val waiting  = outcomes.collect { case (request, (_, _, None)) => request.waiting }.flatten
          val withLast = state.withStaged(last, waiting, at, byUser)
          val staged   = withLast.copy(decided = withLast.decided ++ gone)
          val answers  = outcomes.map { case (request, (_, gone, error)) =>
            request -> error
              .orElse(
                (gone ++ request.notHeld)
                  .headOption
                  .map(d => RecordNotInFlightException(d.offset._1, d.offset._2))
              )
              .toLeft(())
          }
          F.pure((if (sending) staged.withoutDecided else staged) -> answers)
        case (_, Left(error)) =>
          batch.traverse_(_._2(Left(error))) *>
            F.raiseError[(ShareState[F], Vector[(Staging[F], Either[Throwable, Unit])])](error)
      }
  }

  // Java's `commitSync`, which sends every staged decision.
  private def commit(
    state: ShareState[F],
    timeout: FiniteDuration,
    done: Either[Throwable, CommitResult] => F[Unit]
  ): F[ShareState[F]] =
    commitStaged(state, timeout).attempt.flatTap(done).as(state.withoutDecided)

  // In implicit mode, a commit would accept a poll the stream has not taken.
  private def holdsUntakenPoll(state: ShareState[F]): Boolean =
    config.implicitMode && state.unpulled.nonEmpty

  private def commitStaged(state: ShareState[F], timeout: FiniteDuration): F[CommitResult] =
    if (holdsUntakenPoll(state)) F.pure(Map.empty)
    else
      withConsumer
        .blocking(consumer => commitResult(consumer.commitSync(timeout.toJava)))
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

  // Returned renewed records get their waiting decisions; new records are handed out, or released
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
      // Waiting decisions are the user's, sent at once rather than behind the slowest record.
      waiting = returned.flatMap { case (p, _) => next.deferred.get(p).map(p -> _) }
      // One request per waiting decision, to settle its waiters with its own result.
      waitingBatch = waiting.map { case (p, kind) =>
                       Chunk.singleton(decision(next, p, kind)) -> noCallback
                     }
      releases = rejected.map { case (p, _) => decision(next, p, AcknowledgeType.Release) }
      batch    = waitingBatch ++
                (if (releases.isEmpty) Vector.empty else Vector(Chunk.from(releases) -> noCallback))
      staged <-
        if (batch.isEmpty) F.pure(next)
        else
          stageResults(
            next.copy(deferred = next.deferred -- waiting.map(_._1)),
            batch,
            send = true,
            byUser = true
          ).flatMap { case (s, results) =>
            val (waitingResults, releaseResults) = results.map(_._2).splitAt(waiting.size)
            val (settled, effects)               =
              waiting
                .map(_._1)
                .zip(waitingResults)
                .foldLeft((s, Vector.empty[F[Unit]])) { case ((current, done), (p, result)) =>
                  val (next, more) = current.settled(p, result)
                  (next, done ++ more)
                }
            // A failed release fails the actor.
            effects.sequence_ *> releaseResults.traverse_(_.liftTo[F]).as(settled)
          }
    } yield staged
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

  // Undecided records are renewed at once while a decision waits for a renewed record, so that the
  // consumer can poll it, and with automatic renewal at the renewal interval.
  private def nextRenewal(state: ShareState[F]): Option[FiniteDuration] = {
    // Only scans the records when something is to be renewed.
    lazy val undecided = state.undecided.map(_._2.lockedSince)
    if (state.allSettled || (state.deferred.isEmpty && !config.settings.autoRenewal)) None
    else if (state.deferred.nonEmpty) Some(undecided.min)
    // Also after the stream ended, as records may still be processed downstream.
    else if (config.settings.autoRenewal)
      Some(undecided.min + state.lock.fold(unknownLockRenewalInterval)(renewalInterval))
    else None
  }

  // The renewal margin before the lock ends, or half the lock, early enough for
  // `max.poll.interval.ms`. A commit blocks renewals while it waits.
  private def renewalInterval(lock: FiniteDuration): FiniteDuration = {
    val byMargin =
      if (config.settings.renewalMargin < lock) lock - config.settings.renewalMargin else lock / 2
    byMargin.min(config.maxPollInterval / 2)
  }

  private def renew(state: ShareState[F]): F[ShareState[F]] =
    stageOwn(
      state,
      state.undecided.map { case (p, _) => decision(state, p, AcknowledgeType.Renew) },
      byUser = false
    )

  // --- Max acknowledgement delay ---

  // When the staged decisions are sent without waiting for the next poll: at once while paused, as
  // no poll comes, also to accept a taken poll in implicit mode.
  private def nextSend(state: ShareState[F]): Option[FiniteDuration] =
    if (state.paused && state.renewing.isEmpty)
      state
        .stagedSince
        .orElse(if (state.acceptPending && !holdsUntakenPoll(state)) Some(Duration.Zero) else None)
    else (state.stagedSince, config.settings.maxAcknowledgementDelay).mapN(_ + _)

  // Java's `commitAsync`, which sends every staged decision.
  private def sendStaged(state: ShareState[F]): F[ShareState[F]] =
    if (holdsUntakenPoll(state)) F.pure(state)
    else withConsumer.blocking(_.commitAsync()).as(state.withoutDecided)

  // --- Max processing time ---

  // When the max processing time releases each undecided record the stream took, counted from
  // when it took it or the user's latest renewal.
  private def limits(state: ShareState[F]): Vector[(TopicPartitionOffset, FiniteDuration)] =
    config
      .settings
      .maxProcessingTime
      .fold(Vector.empty[(TopicPartitionOffset, FiniteDuration)])(max =>
        state.undecided.collect { case (p, h) if !state.unpulled(p) => p -> (h.heldSince + max) }
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

  // Receives requests in order, staging consecutive decisions in one call.
  private def receiveAll(state: ShareState[F], taken: List[ShareRequest[F]]): F[ShareState[F]] =
    taken match {
      case Nil                             => F.pure(state)
      case (_: ShareRequest.Stage[F]) :: _ =>
        val (stages, rest) = taken.span(_.isInstanceOf[ShareRequest.Stage[F]])
        val batch          = stages.collect { case ShareRequest.Stage(d, done) => d -> done }.toVector
        stageAll(state, batch, send = false, byUser = true).flatMap(receiveAll(_, rest))
      case request :: rest => receive(state, request).flatMap(receiveAll(_, rest))
    }

  // Ends with the result of the final commit after stopping.
  private def loop(state: ShareState[F]): F[Either[Throwable, CommitResult]] =
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
              commitStaged(current, config.settings.commitTimeout).attempt
            else if (overdue.nonEmpty) releaseOverdue(current, overdue).flatMap(loop)
            // Stopping after the stream ended: nothing will decide the rest.
            else if (current.stopping && current.readerClosed && current.undecided.nonEmpty)
              release(current, current.undecided.map(_._1)).flatMap(loop)
            else if (renewal.exists(_ <= now)) renew(current).flatMap(loop)
            // Before polling, which may wait while unsubscribed.
            else if (send.exists(_ <= now)) sendStaged(current).flatMap(loop)
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
  private def withPauses: F[Either[Throwable, CommitResult]] =
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
            case Left(_)       => F.never[Either[Throwable, CommitResult]]
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

  type CommitResult = Map[TopicPartition, Option[KafkaException]]

  // How the actor ended.
  sealed abstract class Ended

  object Ended {

    // After stopping, with the result of the final commit.
    final case class Drained(lastCommit: Either[Throwable, CommitResult]) extends Ended

    // Terminated, or closed before it drained.
    case object Shutdown extends Ended

    final case class Failed(error: Throwable) extends Ended

  }

  def offsetOf(record: KafkaByteConsumerRecord): TopicPartitionOffset =
    new TopicPartition(record.topic, record.partition) -> record.offset

  def deliveryCountOf(record: KafkaByteConsumerRecord): Short =
    if (record.deliveryCount().isPresent) record.deliveryCount().get.shortValue else 1

  // One request's held decisions, to stage now or waiting for a renewed record, and its first
  // decision for a delivery not held.
  final case class Staging[F[_]](
    done: Either[Throwable, Unit] => F[Unit],
    now: Vector[Decision],
    waiting: Vector[Decision],
    notHeld: Option[Decision]
  )

  object Staging {

    def apply[F[_]](
      state: ShareState[F],
      decisions: Chunk[Decision],
      done: Either[Throwable, Unit] => F[Unit]
    ): Staging[F] = {
      val now     = Vector.newBuilder[Decision]
      val waiting = Vector.newBuilder[Decision]
      var notHeld = Option.empty[Decision]
      decisions.foreach { d =>
        if (!state.holds(d.offset, d.deliveryCount)) { if (notHeld.isEmpty) notHeld = Some(d) }
        else if (state.renewing(d.offset)) waiting += d
        else now                                   += d
      }
      Staging(done, now.result(), waiting.result(), notHeld)
    }

  }

  // How often an unsubscribed consumer checks for a subscription.
  val subscriptionCheckInterval: FiniteDuration = 50.millis

  // Half of the shortest lock a broker allows, while the broker has not reported the lock.
  val unknownLockRenewalInterval: FiniteDuration = 500.millis

  private def commitResult(
    results: java.util.Map[TopicIdPartition, java.util.Optional[KafkaException]]
  ): CommitResult =
    results
      .asScala
      .iterator
      .map { case (partition, error) =>
        partition.topicPartition -> (if (error.isPresent) Some(error.get) else None)
      }
      .toMap

  def resource[F[_], K, V](
    settings: ShareConsumerSettings[F, K, V],
    keyDeserializer: KeyDeserializer[F, K],
    valueDeserializer: ValueDeserializer[F, V],
    withConsumer: WithShareConsumer[F],
    id: Int
  )(implicit F: Async[F]): Resource[F, KafkaShareConsumerActor[F, K, V]] =
    for {
      logging  <- Resource.eval(Logging.create[F]("fs2.kafka.KafkaShareConsumer$" + id))
      requests <- Resource.eval(Queue.unbounded[F, ShareRequest[F]])
      output   <-
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
