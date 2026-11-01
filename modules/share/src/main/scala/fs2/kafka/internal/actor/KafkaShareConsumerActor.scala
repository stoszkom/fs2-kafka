/*
 * Copyright 2018 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka.internal.actor

import java.util.concurrent.ConcurrentLinkedQueue

import scala.concurrent.duration.*

import cats.effect.{Async, Deferred, Outcome, Ref, Resource}
import cats.effect.std.Queue
import cats.syntax.all.*
import fs2.{Chunk, Stream}
import fs2.kafka.*
import fs2.kafka.internal.converters.collection.*
import fs2.kafka.internal.syntax.*
import fs2.kafka.internal.WithShareConsumer

import org.apache.kafka.clients.consumer.AcknowledgementCommitCallback
import org.apache.kafka.common.{KafkaException, TopicIdPartition, TopicPartition}
import org.apache.kafka.common.errors.TimeoutException

/**
  * Polls a Java share consumer in explicit acknowledgement mode, and stages each decision in it as
  * it arrives. Java sends staged decisions with the next poll, or with a commit, and lets the last
  * staged decision for a record win until then, so the actor mostly tracks which records of the
  * latest poll have a decision: it polls again once all do.
  *
  * Renewals are the exception, as Java lets a renewed record be decided again only once a poll has
  * returned it. A renewal is sent at once, so the lock is extended while other records are still
  * processed; a decision for a renewed record waits in the actor until a poll returns the record,
  * without holding back its caller, and the returned record is not handed out again.
  */
final private[kafka] class KafkaShareConsumerActor[F[_], K, V](
  config: ShareActorConfig[F, K, V],
  keyDeserializer: KeyDeserializer[F, K],
  valueDeserializer: ValueDeserializer[F, V],
  withConsumer: WithShareConsumer[F],
  requests: Queue[F, KafkaShareConsumerActor.Request[F]],
  output: Queue[F, Option[(Long, Chunk[AcknowledgeableConsumerRecord[F, K, V]])]],
  readerStarted: Ref[F, Boolean],
  shutdown: Deferred[F, Unit],
  finished: Deferred[F, Either[Throwable, Unit]],
  drained: Ref[F, Option[Either[Throwable, Map[TopicPartition, Option[KafkaException]]]]]
)(implicit F: Async[F], logging: ShareLogging[F])
    extends Stager[F] {

  import KafkaShareConsumerActor.*
  import config.{
    apiTimeout,
    batchOptimized,
    implicitMode,
    maxPollInterval,
    maxPollRecords,
    settings
  }

  // Once the actor has finished after stopping, it holds no records, so a decision is for a record
  // not in flight.
  override def stage(decisions: Chunk[Decision]): F[Unit] =
    if (decisions.isEmpty) F.unit
    else if (implicitMode) F.raiseError(ImplicitAcknowledgementException())
    else
      ask[Unit](
        Request.Stage(decisions, _),
        _ =>
          Left(
            RecordNotInFlightException(decisions.head.get.topicPartition, decisions.head.get.offset)
          )
      )

  // Commits every staged decision, as Java's `commitSync` does, with the result per partition.
  // Decisions waiting for a renewed record are staged once a poll returns it, so they are not
  // included. After the actor finished, it is the result of its final commit.
  def commit: F[Map[TopicPartition, Option[KafkaException]]] =
    ask(Request.Commit(_), identity)

  def stopConsuming: F[Unit] = finished
    .tryGet
    .flatMap(_.fold(requests.offer(Request.Stop()))(_ => F.unit))

  def records: Stream[F, AcknowledgeableConsumerRecord[F, K, V]] = polls.flatMap(handOut[F, K, V])

  /**
    * The polls, each with the effect which hands it out. A poll counts as handed out once the
    * stream takes its chunk, so merged polls of several consumers are handed out after the merge.
    */
  def polls: Stream[F, (Chunk[AcknowledgeableConsumerRecord[F, K, V]], F[Unit])] =
    Stream
      .eval(readerStarted.getAndSet(true))
      .flatMap {
        case true  => Stream.raiseError[F](StreamAlreadyStartedException())
        case false =>
          val failed = finished
            .get
            .flatMap(_.fold(e => F.pure(e.asLeft[Unit]), _ => F.never[Either[Throwable, Unit]]))
          Stream.exec(requests.offer(Request.Start())) ++
            Stream
              .fromQueueNoneTerminated(output)
              .map { case (id, chunk) => chunk -> requests.offer(Request.HandedOut(id)) }
              .onFinalize(requests.offer(Request.ReaderClosed()))
              .interruptWhen(failed)
      }

  def awaitTermination: F[Unit] = finished.get.rethrow

  def terminate: F[Unit] = shutdown.complete(()) *> finished.get.void

  // Sends a request and waits for its answer, or for the actor to end: then the answer comes from
  // `afterDrain` with the final commit's result if it finished after stopping, or else it is the
  // reason it ended.
  private def ask[A](
    request: (Either[Throwable, A] => F[Unit]) => Request[F],
    afterDrain: Either[Throwable, Map[TopicPartition, Option[KafkaException]]] => Either[
      Throwable,
      A
    ]
  ): F[A] = {
    def afterEnd(ended: Either[Throwable, Unit]): F[A] =
      drained
        .get
        .flatMap {
          // Also after a failed final commit, which `drained` holds.
          case Some(last) => afterDrain(last).liftTo[F]
          case _          =>
            F.raiseError[A](ended.fold(identity, _ => ConsumerShutdownException()))
        }
    // Once the actor ended, nothing takes requests, so they are not queued.
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

  // Java reports the results of sent decisions to this callback during consumer calls. Only a
  // failed renewal matters to the actor: its record lost its lock, so it is no longer held.
  private val callback: AcknowledgementCommitCallback = (offsets, error) =>
    if (error != null)
      offsets
        .asScala
        .foreach { case (partition, values) =>
          failedRenewals.add(partition.topicPartition -> values.asScala.map(_.longValue).toSet)
        }

  // A renewal whose result Java never reported, so its record never came back, is given up once
  // its lock and Kafka's API timeout have passed: the record is delivered again.
  private def withoutUnreported(
    state: State,
    lock: Option[FiniteDuration],
    now: FiniteDuration
  ): State = {
    val limit = lock.fold(apiTimeout)(_.max(apiTimeout))
    state.withoutRenewing(state.renewing.filter(p => state.held(p).lockedSince + limit <= now))
  }

  private def dropFailedRenewals(state: State): F[State] =
    F.delay(Iterator.continually(failedRenewals.poll()).takeWhile(_ != null).toVector)
      .map(_.foldLeft(state) { case (s, (partition, offsets)) =>
        s.withoutRenewing(offsets.map(partition -> _))
      })

  // --- Requests ---

  private def receive(state: State, request: Request[F]): F[State] =
    request match {
      case Request.Start() =>
        withConsumer
          .blocking(_.subscription().isEmpty)
          .ifM(F.raiseError(NotSubscribedException()), F.pure(state.copy(started = true)))
      case Request.Stop()                 => F.pure(state.copy(stopping = true))
      case Request.HandedOut(id)          => F.monotonic.map(state.withHandedOut(id, _))
      case Request.ReaderClosed()         => releaseUnpulled(state.copy(readerClosed = true))
      case Request.Stage(decisions, done) =>
        stageAll(state, Vector(decisions -> done), send = false, byUser = true)
      case Request.Commit(done) => commit(state, done)
    }

  // Records of a poll the stream never took are released, so other consumers get them; renewing
  // ones once a poll returns them. Java can't release them in implicit mode, where the poll stays
  // unpulled, so that it is not accepted; closing the consumer releases it.
  private def releaseUnpulled(state: State): F[State] = {
    val undecided = state.undecided.map(_._1).toSet
    val unpulled  = state.unpulled.filter(undecided).toVector
    val renewing  = state.unpulled.filter(state.renewing)
    val next      = state.copy(
      unpulled = Set.empty,
      deferred = state.deferred ++ renewing.map(_ -> AcknowledgeType.Release)
    )
    if (implicitMode) F.pure(state)
    else if (unpulled.isEmpty) F.pure(next)
    else
      internal(
        next,
        Chunk.from(unpulled.map(p => decision(state, p, AcknowledgeType.Release))),
        send = true,
        byUser = false
      )
  }

  private def decision(state: State, position: Position, kind: AcknowledgeType): Decision =
    Decision(position._1, position._2, state.held(position).deliveryCount, kind)

  // Stages the actor's own decisions, which fail the actor if they cannot be staged.
  private def internal(
    state: State,
    decisions: Chunk[Decision],
    send: Boolean,
    byUser: Boolean
  ): F[State] =
    stageAll(
      state,
      Vector(decisions -> ((result: Either[Throwable, Unit]) => result.liftTo[F])),
      send,
      byUser
    )

  /**
    * Stages the decisions of several requests, in one call to Java, for deliveries the consumer
    * holds, and fails the others. A decision for a record whose renewal was sent waits for the poll
    * which returns it. Renewals are sent at once with the other staged decisions, as is everything
    * when `send` is set.
    */
  private def stageAll(
    state: State,
    batch: Vector[(Chunk[Decision], Either[Throwable, Unit] => F[Unit])],
    send: Boolean,
    byUser: Boolean
  ): F[State] = {
    def isHeld(d: Decision) = state.held.get(d.position).exists(_.deliveryCount == d.deliveryCount)
    // Each request's decisions to stage now, and those waiting for a renewed record.
    val split = batch.map { case (own, done) =>
      val (waiting, now) = own.toVector.filter(isHeld).partition(d => state.renewing(d.position))
      (own, done, waiting, now)
    }
    val now     = split.flatMap(_._4)
    val sending = send || now.exists(_.acknowledgeType == AcknowledgeType.Renew)
    // Each request is staged on its own, so an error is only that request's: how many of its
    // decisions Java staged, and the error. Whatever was staged is sent, if anything is.
    val calls: F[Vector[(Int, Option[Throwable])]] =
      if (now.isEmpty && !send) F.pure(split.map(_ => (0, None)))
      else
        withConsumer.blocking { consumer =>
          val staged = split.map { case (_, _, _, own) =>
            var count   = 0
            val staging = scala
              .util
              .Try(own.foreach { d =>
                consumer.acknowledge(state.held(d.position).record, d.acknowledgeType.asJava)
                count += 1
              })
            (count, staging.failed.toOption)
          }
          // A failed send means the consumer failed, so it fails the actor.
          if (sending) consumer.commitAsync()
          staged
        }
    (F.monotonic, calls.attempt)
      .tupled
      .flatMap {
        case (at, Right(results)) =>
          // What Java staged is tracked also for a request that failed part-way, so the actor's
          // view stays that of Java. The last decision for each record wins, as in Java.
          val outcomes = split.zip(results)
          val last     = outcomes
            .flatMap { case ((_, _, _, own), (count, _)) => own.take(count) }
            .foldLeft(Map.empty[Position, AcknowledgeType])((m, d) =>
              m.updated(d.position, d.acknowledgeType)
            )
          val waiting = outcomes.collect { case ((_, _, waiting, _), (_, None)) => waiting }.flatten
          val staged  = state.withStaged(last, waiting, at, byUser)
          outcomes
            .traverse_ { case ((own, done, _, _), (_, error)) =>
              done(
                error
                  .orElse(
                    own
                      .find(d => !isHeld(d))
                      .map(d => RecordNotInFlightException(d.topicPartition, d.offset))
                  )
                  .toLeft(())
              )
            }
            .as(if (sending) staged.withoutDecided else staged)
        case (_, Left(error)) =>
          batch.traverse_(_._2(Left(error))) *> F.raiseError[State](error)
      }
  }

  // Java commits every staged decision, which it then no longer holds.
  private def commit(
    state: State,
    done: Either[Throwable, Map[TopicPartition, Option[KafkaException]]] => F[Unit]
  ): F[State] =
    commitSync(state)
      .attempt
      .flatTap(done)
      .map {
        // A commit which failed before sending leaves the decisions staged in Java.
        case Left(error) if !error.isInstanceOf[ShareCommitTimeoutException] => state
        case _                                                               => state.withoutDecided
      }

  // In implicit mode, a poll the stream did not take accepted the records handed out before it,
  // and committing would accept that poll as well, so nothing is committed.
  private def commitSync(state: State): F[Map[TopicPartition, Option[KafkaException]]] =
    if (implicitMode && state.unpulled.nonEmpty) F.pure(Map.empty)
    else
      withConsumer
        .blocking(consumer => commitResult(consumer.commitSync(settings.commitTimeout.toJava)))
        .adaptError { case error: TimeoutException =>
          ShareCommitTimeoutException(settings.commitTimeout, error)
        }

  // --- Polling ---

  private def poll(state: State): F[State] =
    F.monotonic
      .flatMap(start =>
        withConsumer
          .blocking { consumer =>
            if (consumer.subscription().isEmpty) None
            else
              Some(
                consumer.poll(settings.pollTimeout.toJava).iterator().asScala.toVector ->
                  ShareTimeouts.acquisitionLock(consumer)
              )
          }
          .flatMap {
            // No fetch is sent while unsubscribed, so the next poll's records are acquired by it.
            // Requests are received while waiting to check the subscription again.
            case None =>
              val idle = state.copy(lastPoll = None)
              takeWithin(idleInterval).flatMap(_.fold(F.pure(idle))(receive(idle, _)))
            case Some((records, reported)) =>
              val lock = reported.orElse(state.lock)
              // With automatic renewal, the commit timeout must leave time to renew behind a commit.
              // Failed renewals are applied first, so a redelivery in this poll is a new delivery.
              ShareTimeouts.effectiveCommitTimeout(settings, lock).liftTo[F] *>
                (F.monotonic, dropFailedRenewals(state))
                  .tupled
                  .flatMap { case (now, current) =>
                    val acquiredAt = acquisitionTime(current, now)
                    polled(current, records, lock, acquiredAt, now).map(next =>
                      withoutUnreported(next, lock, now).copy(lastPoll =
                        Some(LastPoll(start, records.size, acquiredAt))
                      )
                    )
                  }
          }
      )

  // Renewed records come back from the poll without being handed out again; their waiting
  // decisions are staged. New records are handed out, or released once consuming has stopped.
  private def polled(
    state: State,
    records: Vector[KafkaByteConsumerRecord],
    lock: Option[FiniteDuration],
    acquiredAt: FiniteDuration,
    now: FiniteDuration
  ): F[State] = {
    def position(r: KafkaByteConsumerRecord): Position =
      new TopicPartition(r.topic, r.partition) -> r.offset
    val (returned, fresh) = records.partition(r =>
      state.renewing.contains(position(r)) &&
        state.held.get(position(r)).exists(_.deliveryCount == deliveryCountOf(r))
    )
    // A poll made before the stream took the latest one returns only renewed records, so new ones
    // are released in that case too, should Java ever return some.
    val admitting = !state.stopping && !state.readerClosed && state.unpulled.isEmpty
    for {
      admitted <-
        if (admitting) fresh.traverse(admit(_, acquiredAt, now)) else F.pure(Vector.empty)
      next = state.polled(
               // A renewed record's lock runs from when its renewal was sent.
               returned.map(r => position(r) -> state.held(position(r)).copy(record = r)),
               admitted.map(_._1),
               fresh.map(position),
               lock
             )
      _ <- logging
             .log(ShareLogEntry.Polled(records.size, returned.size, next.held.size))
             .whenA(records.nonEmpty)
      _ <-
        output.offer(Some(next.pollId -> Chunk.from(admitted.map(_._2)))).whenA(admitted.nonEmpty)
      // Records not admitted are released at once, as nothing will decide them.
      released <-
        if (admitting || fresh.isEmpty) F.pure(next)
        else
          withConsumer
            .blocking { c =>
              fresh.foreach(c.acknowledge(_, AcknowledgeType.Release.asJava))
              c.commitAsync()
            }
            .as(next)
      waiting = returned.flatMap(r => next.deferred.get(position(r)).map(position(r) -> _))
      // Decisions which waited for a renewal are sent at once, as their records may otherwise wait
      // for the slowest record of the poll.
      staged <-
        if (waiting.isEmpty) F.pure(released)
        else
          internal(
            released.copy(deferred = released.deferred -- waiting.map(_._1)),
            Chunk.from(waiting.map { case (p, kind) => decision(released, p, kind) }),
            send = true,
            // They are the user's decisions, so a renewal restarts the time held.
            byUser = true
          )
    } yield staged
  }

  // The previous poll's fetch may have acquired the records: an empty poll's fetch can complete
  // after it returns, and a full batch_optimized poll can leave records in Java's buffer, also those
  // acquired before it, if the poll before it was full too.
  private def acquisitionTime(state: State, now: FiniteDuration): FiniteDuration =
    state
      .lastPoll
      .collect {
        case LastPoll(start, 0, _)                                           => start
        case LastPoll(start, n, at) if batchOptimized && n >= maxPollRecords => start.min(at)
      }
      .getOrElse(now)

  private def deliveryCountOf(record: KafkaByteConsumerRecord): Short =
    if (record.deliveryCount().isPresent) record.deliveryCount().get.shortValue else 1

  private def admit(
    record: KafkaByteConsumerRecord,
    acquiredAt: FiniteDuration,
    now: FiniteDuration
  ): F[((Position, Held), AcknowledgeableConsumerRecord[F, K, V])] = {
    val deliveryCount = deliveryCountOf(record)
    val partition     = new TopicPartition(record.topic, record.partition)
    ConsumerRecord
      .fromJava(record, keyDeserializer, valueDeserializer)
      .map(decoded =>
        (partition -> record.offset, Held(deliveryCount, record, acquiredAt, now)) ->
          AcknowledgeableConsumerRecord(
            decoded,
            AcknowledgeableOffset(partition, record.offset, deliveryCount, this)
          )
      )
  }

  // --- Automatic renewal ---

  // Records without a decision are renewed at half the lock, when automatic renewal is on, or
  // while a decision waits for a renewed record, so that the consumer can poll it.
  private def nextRenewal(state: State): Option[FiniteDuration] =
    if (!(settings.autoRenewal && !state.readerClosed || state.deferred.nonEmpty)) None
    else {
      val undecided = state.undecided.map(_._2.lockedSince)
      // While a decision waits for a renewed record, the others are renewed promptly, so that the
      // consumer can poll it. While the broker has not reported the lock, records are renewed at
      // the idle interval.
      if (undecided.isEmpty) None
      else if (state.deferred.nonEmpty) Some(undecided.min + idleInterval)
      else Some(undecided.min + state.lock.fold(idleInterval)(renewalInterval))
    }

  // Early enough that a renewal waiting behind a commit still beats the lock, and that the
  // consumer, which polls once the renewal is sent, stays within `max.poll.interval.ms`.
  private def renewalInterval(lock: FiniteDuration): FiniteDuration = {
    val latest = ShareTimeouts.latestRenewal(lock, settings.commitTimeout)
    val bound  = if (latest > Duration.Zero) (lock / 2).min(latest) else lock / 2
    bound.min(maxPollInterval / 2)
  }

  private def renew(state: State): F[State] =
    internal(
      state,
      Chunk.from(state.undecided.map { case (p, _) => decision(state, p, AcknowledgeType.Renew) }),
      send = false,
      byUser = false
    )

  // --- Max processing time ---

  // Records without a decision held for longer than the max processing time, counted from when
  // the stream took them or from the user's latest renewal.
  private def overdueRecords(state: State, now: FiniteDuration): Vector[Position] =
    settings
      .maxProcessingTime
      .fold(Vector.empty[Position])(max =>
        state
          .undecided
          .collect {
            case (p, h) if !state.unpulled(p) && h.heldSince + max <= now => p
          }
      )

  private def nextLimit(state: State): Option[FiniteDuration] =
    settings
      .maxProcessingTime
      .flatMap { max =>
        val held = state.undecided.collect { case (p, h) if !state.unpulled(p) => h.heldSince }
        if (held.isEmpty) None else Some(held.min + max)
      }

  // Overdue records are released at once, so they are delivered again and the consumer can poll.
  private def releaseOverdue(state: State, records: Vector[Position]): F[State] =
    settings
      .maxProcessingTime
      .traverse_(max => logging.log(ShareLogEntry.ReleasingOverdue(records, max))) *>
      internal(
        state,
        Chunk.from(records.map(decision(state, _, AcknowledgeType.Release))),
        send = true,
        byUser = false
      )

  // --- Running ---

  // Takes a request, or returns `None` after the duration, without losing a request taken as the
  // timer fires.
  private def takeWithin(duration: FiniteDuration): F[Option[Request[F]]] =
    F.racePair(requests.take, F.sleep(duration))
      .flatMap {
        case Left((taken, timer)) => timer.cancel *> taken.embedNever.map(Some(_))
        case Right((taker, _))    =>
          taker.cancel *> taker
            .join
            .flatMap {
              case Outcome.Succeeded(request) => request.map(Some(_))
              case Outcome.Errored(error)     => F.raiseError(error)
              case Outcome.Canceled()         => F.pure(None)
            }
      }

  // Requests taken together are received in order, with consecutive decisions staged in one call.
  private def receiveAll(state: State, taken: List[Request[F]]): F[State] =
    taken match {
      case Nil                        => F.pure(state)
      case (_: Request.Stage[F]) :: _ =>
        val (stages, rest) = taken.span(_.isInstanceOf[Request.Stage[F]])
        val batch          = stages.collect { case Request.Stage(d, done) => d -> done }.toVector
        stageAll(state, batch, send = false, byUser = true).flatMap(receiveAll(_, rest))
      case request :: rest => receive(state, request).flatMap(receiveAll(_, rest))
    }

  private def loop(state: State): F[Unit] =
    requests
      .tryTakeN(None)
      .flatMap(receiveAll(state, _))
      .flatMap(dropFailedRenewals)
      .flatMap { current =>
        F.monotonic
          .flatMap { now =>
            val renewal = nextRenewal(current)
            val overdue = overdueRecords(current, now)
            val wakeAt  = (renewal.toList ++ nextLimit(current)).reduceOption(_ min _)
            // The last decisions are sent before finishing, so a later commit is not needed.
            if (current.isFinished)
              // A failed final commit fails the stream, as its decisions may not have been sent.
              commitSync(current)
                .attempt
                .flatMap(result => drained.set(Some(result)) *> result.liftTo[F].void)
            else if (overdue.nonEmpty) releaseOverdue(current, overdue).flatMap(loop)
            else if (renewal.exists(_ <= now)) renew(current).flatMap(loop)
            else if (current.canPoll) poll(current).flatMap(loop)
            else
              wakeAt
                .fold(requests.take.map(Option(_)))(at => takeWithin(at - now))
                .flatMap(_.fold(F.pure(current))(receive(current, _)))
                .flatMap(loop)
          }
      }

  // Terminating ends the stream without draining, so later requests fail.
  private[actor] def run: F[Unit] =
    F.race(
        shutdown.get,
        withConsumer.blocking(_.setAcknowledgementCommitCallback(callback)) *>
          loop(State(implicitMode = implicitMode))
      )
      .attempt
      .flatMap {
        case Right(_)    => output.offer(None) *> finished.complete(Right(())).void
        case Left(error) => finished.complete(Left(error)).void
      }

}

private[kafka] object KafkaShareConsumerActor {

  type Position = TopicPartitionOffset

  // Hands out a poll as the stream takes its chunk.
  def handOut[F[_], K, V](
    poll: (Chunk[AcknowledgeableConsumerRecord[F, K, V]], F[Unit])
  ): Stream[F, AcknowledgeableConsumerRecord[F, K, V]] =
    Stream.exec(poll._2) ++ Stream.chunk(poll._1)

  // How long the actor waits before checking for a subscription again after `unsubscribe`.
  val idleInterval: FiniteDuration = 50.millis

  private def commitResult(
    results: java.util.Map[TopicIdPartition, java.util.Optional[KafkaException]]
  ): Map[TopicPartition, Option[KafkaException]] =
    results
      .asScala
      .iterator
      .map { case (partition, error) =>
        partition.topicPartition -> (if (error.isPresent) Some(error.get) else None)
      }
      .toMap

  /**
    * A delivery of a record held by the consumer.
    *
    * @param record
    *   the Java record to acknowledge it by, from the poll which last returned it
    * @param lockedSince
    *   when its lock started as far as the consumer can tell: when a poll returned it, or the
    *   previous poll started if that poll's fetch may have acquired it, or when its latest renewal
    *   was sent
    * @param heldSince
    *   when its processing started as far as the consumer can tell: when the stream took its poll,
    *   or when the user last renewed it; the max processing time counts from it
    */
  final case class Held(
    deliveryCount: Short,
    record: KafkaByteConsumerRecord,
    lockedSince: FiniteDuration,
    heldSince: FiniteDuration
  )

  final case class LastPoll(start: FiniteDuration, records: Int, acquiredAt: FiniteDuration)

  sealed abstract class Request[F[_]]

  object Request {

    final case class Start[F[_]]() extends Request[F]

    final case class Stop[F[_]]() extends Request[F]

    final case class ReaderClosed[F[_]]() extends Request[F]

    final case class HandedOut[F[_]](pollId: Long) extends Request[F]

    final case class Stage[F[_]](
      decisions: Chunk[Decision],
      done: Either[Throwable, Unit] => F[Unit]
    ) extends Request[F]

    final case class Commit[F[_]](
      done: Either[Throwable, Map[TopicPartition, Option[KafkaException]]] => F[Unit]
    ) extends Request[F]

  }

  /**
    * @param implicitMode
    *   whether Java accepts a poll's records with the next poll or commit
    * @param held
    *   the records the consumer holds: those of the latest poll Java has not sent a decision for,
    *   and renewed records it has not returned yet
    * @param decided
    *   held records with a staged decision, sent by the next poll or commit
    * @param renewing
    *   held records whose renewal was sent, which Java returns from a later poll
    * @param deferred
    *   decisions for renewing records, staged once a poll returns them
    * @param lock
    *   the acquisition lock last reported by the broker
    * @param unpulled
    *   the records of the latest poll while the stream has not taken it
    * @param pollId
    *   the id of the latest poll offered to the stream, so that a late hand-out of an earlier one
    *   is told apart
    * @param lastPoll
    *   when the previous poll started, how many records it returned, and when they were acquired as
    *   far as the consumer can tell
    */
  final case class State(
    implicitMode: Boolean,
    held: Map[Position, Held] = Map.empty,
    decided: Set[Position] = Set.empty,
    renewing: Set[Position] = Set.empty,
    deferred: Map[Position, AcknowledgeType] = Map.empty,
    lock: Option[FiniteDuration] = None,
    unpulled: Set[Position] = Set.empty,
    pollId: Long = 0L,
    lastPoll: Option[LastPoll] = None,
    started: Boolean = false,
    stopping: Boolean = false,
    readerClosed: Boolean = false
  ) {

    // Held records without a decision or a renewal.
    lazy val undecided: Vector[(Position, Held)] =
      held.iterator.filterNot { case (p, _) => decided(p) || renewing(p) }.toVector

    private def settled: Boolean = undecided.isEmpty

    // Java polls once the stream took the latest poll, and every record of it has a decision or a
    // renewal; in implicit mode, the poll accepts them. Renewed records come back only from a
    // poll, so the consumer also polls for them after stopping, or before the stream took the
    // latest poll, as Java returns no new records while a renewal is pending.
    def canPoll: Boolean =
      started && settled && (unpulled.isEmpty || renewing.nonEmpty) &&
        (!stopping && !readerClosed || renewing.nonEmpty)

    // After stopping, the stream ends once every record is decided, or in implicit mode, once it
    // took the latest poll, which the final commit accepts, or ended without taking it. After the
    // stream ends on its own, the actor keeps staging and committing decisions until the consumer
    // closes, which releases records without one.
    def isFinished: Boolean =
      stopping && held.keySet.subsetOf(decided) &&
        (!implicitMode || unpulled.isEmpty || readerClosed)

    // Renewals restart the records' locks at `at`, when they are sent, and a renewal by the user
    // also restarts the time they are held for.
    def withStaged(
      last: Map[Position, AcknowledgeType],
      waiting: Vector[Decision],
      at: FiniteDuration,
      byUser: Boolean
    ): State = {
      val (renewed, final_) = last.partition(_._2 == AcknowledgeType.Renew)
      copy(
        held = held ++ renewed
          .keySet
          .map { p =>
            p -> held(p).copy(lockedSince = at, heldSince = if (byUser) at else held(p).heldSince)
          },
        decided = decided ++ final_.keySet -- renewed.keySet,
        renewing = renewing ++ renewed.keySet,
        deferred = deferred ++ waiting.map(d => d.position -> d.acknowledgeType)
      )
    }

    // A poll sent the staged decisions: held are the renewed records it returned, the renewing
    // records it did not return yet, and its admitted new records.
    def polled(
      returned: Vector[(Position, Held)],
      admitted: Vector[(Position, Held)],
      fresh: Vector[Position],
      reportedLock: Option[FiniteDuration]
    ): State = {
      // Java returns new records only once no renewal is pending, so renewed records a poll with
      // new records did not return lost their lock.
      val stillRenewing =
        if (fresh.nonEmpty) Set.empty[Position] else renewing -- returned.map(_._1)
      val lost = renewing -- returned.map(_._1) -- stillRenewing
      copy(
        // In implicit mode, records are accepted rather than decided, so none are held.
        held = held.filter { case (p, _) => stillRenewing(p) } ++ returned ++
          (if (implicitMode) Vector.empty else admitted),
        decided = Set.empty,
        renewing = stillRenewing,
        deferred = deferred -- fresh -- lost,
        lock = reportedLock,
        unpulled = if (admitted.isEmpty) unpulled else admitted.map(_._1).toSet,
        pollId = if (admitted.isEmpty) pollId else pollId + 1
      )
    }

    // The stream took the poll's chunk; a late hand-out of an earlier poll changes nothing.
    // Records are held, for the max processing time, from when the stream took them.
    def withHandedOut(id: Long, at: FiniteDuration): State =
      if (id != pollId) this
      else
        copy(
          held = held ++ unpulled.flatMap(p => held.get(p).map(h => p -> h.copy(heldSince = at))),
          unpulled = Set.empty
        )

    // A commit sent the staged decisions.
    def withoutDecided: State = copy(held = held -- decided, decided = Set.empty)

    // The renewal failed, so the records lost their lock.
    def withoutRenewing(positions: Set[Position]): State = {
      val lost = positions.intersect(renewing)
      if (lost.isEmpty) this
      else copy(held = held -- lost, renewing = renewing -- lost, deferred = deferred -- lost)
    }

  }

  def resource[F[_], K, V](
    settings: ShareConsumerSettings[F, K, V],
    keyDeserializer: KeyDeserializer[F, K],
    valueDeserializer: ValueDeserializer[F, V],
    withConsumer: WithShareConsumer[F],
    id: Int
  )(implicit F: Async[F]): Resource[F, KafkaShareConsumerActor[F, K, V]] =
    for {
      logging  <- Resource.eval(ShareLogging.create[F]("fs2.kafka.KafkaShareConsumer$" + id))
      requests <- Resource.eval(Queue.unbounded[F, Request[F]])
      output   <-
        Resource.eval(
          Queue.unbounded[F, Option[(Long, Chunk[AcknowledgeableConsumerRecord[F, K, V]])]]
        )
      readerStarted <- Resource.eval(Ref.of[F, Boolean](false))
      shutdown      <- Resource.eval(Deferred[F, Unit])
      finished      <- Resource.eval(Deferred[F, Either[Throwable, Unit]])
      drained       <-
        Resource.eval(
          Ref.of[F, Option[Either[Throwable, Map[TopicPartition, Option[KafkaException]]]]](None)
        )
      config <- Resource.eval(ShareActorConfig.from(settings))
      actor   = new KafkaShareConsumerActor(
                config,
                keyDeserializer,
                valueDeserializer,
                withConsumer,
                requests,
                output,
                readerStarted,
                shutdown,
                finished,
                drained
              )(F, logging)
      // Once closed, requests fail with ConsumerShutdownException, also after draining.
      _ <- Resource.make(F.start(actor.run))(fiber =>
             fiber.cancel *> drained.set(None) *>
               finished.complete(Left(ConsumerShutdownException())).void
           )
    } yield actor

}
