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
  finished: Deferred[F, Either[Throwable, Unit]],
  drained: Ref[F, Option[Either[Throwable, KafkaShareConsumerActor.CommitResult]]],
  waiters: Ref[F, Vector[KafkaShareConsumerActor.Waiter[F]]]
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
            RecordNotInFlightException(decisions.head.get.topicPartition, decisions.head.get.offset)
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
              case Left(_: ConsumerShutdownException) => F.pure(().asRight[Throwable])
              case Left(error)                        => F.pure(error.asLeft[Unit])
              case Right(_)                           => F.never[Either[Throwable, Unit]]
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
        case Left(_: ConsumerShutdownException) => F.unit
        case ended                              => ended.liftTo[F]
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
    def afterEnd(ended: Either[Throwable, Unit]): F[A] =
      (shutdown.tryGet, drained.get)
        .tupled
        .flatMap {
          case (Some(_), _)    => F.raiseError[A](ConsumerShutdownException())
          case (_, Some(last)) => afterDrain(last).liftTo[F]
          case _               =>
            F.raiseError[A](ended.fold(identity, _ => ConsumerShutdownException()))
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
  private def withoutUnreported(state: ShareState, now: FiniteDuration): ShareState = {
    val limit = state.lock.fold(config.apiTimeout)(_ + config.apiTimeout)
    state.withoutRenewing(state.renewing.filter(p => state.held(p).lockedSince + limit <= now))
  }

  private def dropFailedRenewals(state: ShareState): F[ShareState] =
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

  // Completes the requests waiting for a decision on the position with its staging result.
  private def settle(position: Position)(result: Either[Throwable, Unit]): F[Unit] =
    waiters
      .modify { all =>
        val (hit, rest)   = all.partition(_.positions(position))
        val left          = hit.map(w => w.copy(positions = w.positions - position))
        val (ready, keep) =
          if (result.isLeft) (hit, Vector.empty) else left.partition(_.positions.isEmpty)
        (rest ++ keep, ready.traverse_(_.done(result)))
      }
      .flatten

  // A waiting decision whose record lost its lock before a poll returned it.
  private def settleLost(state: ShareState): F[Unit] =
    waiters
      .get
      .flatMap(
        _.flatMap(_.positions)
          .distinct
          .filterNot(state.deferred.contains)
          .traverse_(p => settle(p)(Left(RecordNotInFlightException(p._1, p._2))))
      )

  // --- Requests ---

  private def receive(state: ShareState, request: ShareRequest[F]): F[ShareState] =
    request match {
      case ShareRequest.Start() =>
        withConsumer
          .blocking(_.subscription().isEmpty)
          .ifM(F.raiseError(NotSubscribedException()), F.pure(state.copy(started = true)))
      case ShareRequest.Stop()                 => F.pure(state.copy(stopping = true))
      case ShareRequest.HandedOut(id)          => F.monotonic.map(state.withHandedOut(id, _))
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
  private def releaseUnpulled(state: ShareState): F[ShareState] =
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

  private def release(state: ShareState, positions: Vector[Position]): F[ShareState] =
    if (positions.isEmpty) F.pure(state)
    else stageOwn(state, positions.map(decision(state, _, AcknowledgeType.Release)), byUser = false)

  private def decision(state: ShareState, position: Position, kind: AcknowledgeType): Decision =
    Decision(position._1, position._2, state.held(position).deliveryCount, kind)

  // Stages and sends the actor's own decisions; a failure fails the actor. `byUser` renewals
  // restart the time held.
  private def stageOwn(
    state: ShareState,
    decisions: Vector[Decision],
    byUser: Boolean
  ): F[ShareState] =
    stageAll(
      state,
      Vector(Chunk.from(decisions) -> ((result: Either[Throwable, Unit]) => result.liftTo[F])),
      send = true,
      byUser
    )

  // Stages several requests' decisions in one call to Java. Decisions for renewing records wait for
  // the poll which returns them; renewals, or everything if `send` is set, are sent at once.
  private def stageAll(
    state: ShareState,
    batch: Vector[(Chunk[Decision], Either[Throwable, Unit] => F[Unit])],
    send: Boolean,
    byUser: Boolean
  ): F[ShareState] = {
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
                      consumer.acknowledge(state.held(d.position).record, d.acknowledgeType.asJava)
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
            .map(d => d.position -> d.acknowledgeType)
            .toMap
          val gone     = outcomes.flatMap { case (_, (_, gone, _)) => gone.map(_.position) }.toSet
          val waiting  = outcomes.collect { case (request, (_, _, None)) => request.waiting }.flatten
          val withLast = state.withStaged(last, waiting, at, byUser)
          val staged   = withLast.copy(decided = withLast.decided ++ gone)
          outcomes
            .traverse_ { case (request, (_, gone, error)) =>
              val result = error
                .orElse(
                  (gone ++ request.notHeld)
                    .headOption
                    .map(d => RecordNotInFlightException(d.topicPartition, d.offset))
                )
                .toLeft(())
              // Completes once a poll returns the renewing records and Java staged their decisions.
              if (result.isRight && request.waiting.nonEmpty)
                waiters.update(_ :+ Waiter(request.waiting.map(_.position).toSet, request.done))
              else request.done(result)
            }
            .as(if (sending) staged.withoutDecided else staged)
        case (_, Left(error)) =>
          batch.traverse_(_._2(Left(error))) *> F.raiseError[ShareState](error)
      }
  }

  // Java's `commitSync`, which sends every staged decision.
  private def commit(
    state: ShareState,
    timeout: FiniteDuration,
    done: Either[Throwable, CommitResult] => F[Unit]
  ): F[ShareState] =
    commitStaged(state, timeout).attempt.flatTap(done).as(state.withoutDecided)

  // In implicit mode, a commit would accept a poll the stream has not taken.
  private def holdsUntakenPoll(state: ShareState): Boolean =
    config.implicitMode && state.unpulled.nonEmpty

  private def commitStaged(state: ShareState, timeout: FiniteDuration): F[CommitResult] =
    if (holdsUntakenPoll(state)) F.pure(Map.empty)
    else
      withConsumer
        .blocking(consumer => commitResult(consumer.commitSync(timeout.toJava)))
        .adaptError { case error: TimeoutException =>
          ShareCommitTimeoutException(timeout, error)
        }

  // --- Polling ---

  private def poll(state: ShareState): F[ShareState] =
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
    state: ShareState,
    records: Vector[KafkaByteConsumerRecord],
    lock: Option[FiniteDuration],
    acquiredAt: FiniteDuration,
    now: FiniteDuration
  ): F[ShareState] = {
    val positioned        = records.map(r => position(r) -> r)
    val (returned, fresh) = positioned.partition { case (p, r) =>
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
      waiting      = returned.flatMap { case (p, _) => next.deferred.get(p).map(p -> _) }
      waitingBatch =
        waiting.map { case (p, kind) =>
          Chunk.singleton(decision(next, p, kind)) -> ((r: Either[Throwable, Unit]) => settle(p)(r))
        }
      releases = rejected.map { case (p, _) => decision(next, p, AcknowledgeType.Release) }
      batch    = waitingBatch ++
                (if (releases.isEmpty) Vector.empty
                 else
                   Vector(
                     Chunk.from(releases) -> ((r: Either[Throwable, Unit]) => r.liftTo[F])
                   ))
      staged <-
        if (batch.isEmpty) F.pure(next)
        else
          stageAll(
            next.copy(deferred = next.deferred -- waiting.map(_._1)),
            batch,
            send = true,
            byUser = true
          )
    } yield staged
  }

  private def held(
    record: KafkaByteConsumerRecord,
    acquiredAt: FiniteDuration,
    now: FiniteDuration
  ) =
    Held(deliveryCountOf(record), record, acquiredAt, now)

  private def admit(
    position: Position,
    record: KafkaByteConsumerRecord,
    acquiredAt: FiniteDuration,
    now: FiniteDuration
  ): F[((Position, Held), AcknowledgeableConsumerRecord[F, K, V])] = {
    val delivery = held(record, acquiredAt, now)
    ConsumerRecord
      .fromJava(record, keyDeserializer, valueDeserializer)
      .map(decoded =>
        (position -> delivery) ->
          AcknowledgeableConsumerRecord(
            decoded,
            AcknowledgeableOffset(position._1, position._2, delivery.deliveryCount, this)
          )
      )
  }

  // --- Automatic renewal ---

  // Undecided records are renewed at once while a decision waits for a renewed record, so that the
  // consumer can poll it, and with automatic renewal at the renewal interval.
  private def nextRenewal(state: ShareState): Option[FiniteDuration] = {
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

  private def renew(state: ShareState): F[ShareState] =
    stageOwn(
      state,
      state.undecided.map { case (p, _) => decision(state, p, AcknowledgeType.Renew) },
      byUser = false
    )

  // --- Max acknowledgement delay ---

  // When the staged decisions are sent without waiting for the next poll.
  private def nextSend(state: ShareState): Option[FiniteDuration] =
    (state.stagedSince, config.settings.maxAcknowledgementDelay).mapN(_ + _)

  // Java's `commitAsync`, which sends every staged decision.
  private def sendStaged(state: ShareState): F[ShareState] =
    if (holdsUntakenPoll(state)) F.pure(state)
    else withConsumer.blocking(_.commitAsync()).as(state.withoutDecided)

  // --- Max processing time ---

  // When the max processing time releases each undecided record the stream took, counted from
  // when it took it or the user's latest renewal.
  private def limits(state: ShareState): Vector[(Position, FiniteDuration)] =
    config
      .settings
      .maxProcessingTime
      .fold(Vector.empty[(Position, FiniteDuration)])(max =>
        state.undecided.collect { case (p, h) if !state.unpulled(p) => p -> (h.heldSince + max) }
      )

  // Releases overdue records, so they are delivered again and the consumer can poll.
  private def releaseOverdue(state: ShareState, records: Vector[Position]): F[ShareState] =
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
  private def receiveAll(state: ShareState, taken: List[ShareRequest[F]]): F[ShareState] =
    taken match {
      case Nil                             => F.pure(state)
      case (_: ShareRequest.Stage[F]) :: _ =>
        val (stages, rest) = taken.span(_.isInstanceOf[ShareRequest.Stage[F]])
        val batch          = stages.collect { case ShareRequest.Stage(d, done) => d -> done }.toVector
        stageAll(state, batch, send = false, byUser = true).flatMap(receiveAll(_, rest))
      case request :: rest => receive(state, request).flatMap(receiveAll(_, rest))
    }

  private def loop(state: ShareState): F[Unit] =
    requests
      .tryTakeN(None)
      .flatMap(receiveAll(state, _))
      .flatMap(dropFailedRenewals)
      .flatTap(settleLost)
      .flatMap { current =>
        F.monotonic
          .flatMap { now =>
            val renewal = nextRenewal(current)
            val limit   = limits(current)
            val overdue = limit.collect { case (p, at) if at <= now => p }
            val send    = nextSend(current)
            val wakeAt  = (renewal.toList ++ send ++ limit.map(_._2)).reduceOption(_ min _)
            if (current.isFinished)
              // A failed final commit fails the stream.
              commitStaged(current, config.settings.commitTimeout)
                .attempt
                .flatMap(result => drained.set(Some(result)) *> result.liftTo[F].void)
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

  // Terminating ends without draining, so later requests fail.
  private[actor] def run: F[Unit] =
    F.race(
        shutdown.get,
        withConsumer.blocking(_.setAcknowledgementCommitCallback(callback)) *>
          loop(ShareState())
      )
      .attempt
      .flatMap {
        case Right(_)    => output.offer(None) *> finished.complete(Right(())).void
        case Left(error) => finished.complete(Left(error)).void
      }

}

private[kafka] object KafkaShareConsumerActor {

  type Position = TopicPartitionOffset

  type CommitResult = Map[TopicPartition, Option[KafkaException]]

  def position(record: KafkaByteConsumerRecord): Position =
    new TopicPartition(record.topic, record.partition) -> record.offset

  def deliveryCountOf(record: KafkaByteConsumerRecord): Short =
    if (record.deliveryCount().isPresent) record.deliveryCount().get.shortValue else 1

  // A request completed once its decisions for renewing records are staged.
  final case class Waiter[F[_]](
    positions: Set[Position],
    done: Either[Throwable, Unit] => F[Unit]
  )

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
      state: ShareState,
      decisions: Chunk[Decision],
      done: Either[Throwable, Unit] => F[Unit]
    ): Staging[F] = {
      val now     = Vector.newBuilder[Decision]
      val waiting = Vector.newBuilder[Decision]
      var notHeld = Option.empty[Decision]
      decisions.foreach { d =>
        if (!state.holds(d.position, d.deliveryCount)) { if (notHeld.isEmpty) notHeld = Some(d) }
        else if (state.renewing(d.position)) waiting += d
        else now                                     += d
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
      finished      <- Resource.eval(Deferred[F, Either[Throwable, Unit]])
      drained       <- Resource.eval(Ref.of[F, Option[Either[Throwable, CommitResult]]](None))
      waiters       <- Resource.eval(Ref.of[F, Vector[Waiter[F]]](Vector.empty))
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
                finished,
                drained,
                waiters
              )(F, logging)
      // Once closed, requests fail with ConsumerShutdownException.
      _ <- Resource.make(F.start(actor.run))(fiber =>
             fiber.cancel *> shutdown.complete(()) *>
               finished.complete(Left(ConsumerShutdownException())).void
           )
    } yield actor

}
