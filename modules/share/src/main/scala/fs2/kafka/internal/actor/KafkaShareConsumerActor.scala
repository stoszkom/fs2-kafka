/*
 * Copyright 2018 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka.internal.actor

import scala.concurrent.duration.*

import cats.effect.{Async, Deferred, Outcome, Ref, Resource}
import cats.effect.std.Queue
import cats.syntax.all.*
import fs2.{Chunk, Stream}
import fs2.kafka.*
import fs2.kafka.internal.converters.collection.*
import fs2.kafka.internal.syntax.*
import fs2.kafka.internal.WithShareConsumer

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.errors.TimeoutException
import org.apache.kafka.common.TopicPartition

/**
  * [[KafkaShareConsumerActor]] wraps a Java `KafkaShareConsumer` and works like a traditional
  * actor: it receives requests one-at-a-time via a queue. It hands out each poll as one chunk and
  * commits acknowledgements as they arrive.
  *
  * ==Polling==
  *
  * In explicit mode, the actor polls once every record of the previous poll is acknowledged.
  * Records polled after stopping, or never handed out before the stream ended, are released.
  *
  * ==Commit flow==
  *
  * Commits run in rounds, see [[processCommits]]. A round is sent without waiting for its results,
  * which Java reports to one callback during later consumer calls; up to `maxSentRounds` rounds of
  * commit requests wait for results at once. Only a failure to send the actor's own renewals and
  * releases fails the stream.
  *
  * ==Renewal flow==
  *
  * With automatic renewal, unacknowledged records are renewed before their lock expires, so the
  * actor can poll; Java returns them from the next poll. Each step, the actor does one of two
  * things: renew records whose time has come, or process the commits users requested. A timed-out
  * renewal may still complete, so its record is kept.
  *
  * ==Implicit acknowledgement==
  *
  * See [[implicitStep]].
  */
final private[kafka] class KafkaShareConsumerActor[F[_], K, V] private (
  settings: ShareConsumerSettings[F, K, V],
  keyDeserializer: KeyDeserializer[F, K],
  valueDeserializer: ValueDeserializer[F, V],
  withConsumer: WithShareConsumer[F],
  requests: Queue[F, ShareRequest[F]],
  output: Queue[F, Option[Chunk[KafkaShareConsumerActor.Entry[F, K, V]]]],
  pendingRef: Ref[F, Int],
  readerStarted: Ref[F, Boolean],
  shutdown: Deferred[F, Unit],
  finished: Deferred[F, Either[Throwable, Unit]],
  maxPollInterval: FiniteDuration,
  apiTimeout: FiniteDuration,
  implicitMode: Boolean,
  batchOptimized: Boolean,
  maxPollRecords: Int
)(implicit F: Async[F]) {

  import KafkaShareConsumerActor.*
  import ShareCommits.*
  import ShareTimeouts.*

  private type State = ShareState[F]

  private val client = new ShareCommitClient[F](withConsumer)

  // Whether Java accepts a poll's records once they are handed out.
  private[kafka] val implicitAcknowledgement: Boolean = implicitMode

  val committer: KafkaShareCommitter[F] =
    KafkaShareCommitter(
      commitOffsets = offsets => commit(fromOffsets(offsets), sync = false),
      commitDeliveries = offsets => commit(fromAcknowledged(offsets), sync = false),
      commitDeliveriesWithResults =
        offsets => commitWithResults(fromAcknowledged(offsets), sync = false),
      pendingRecords = pendingRef.get
    )

  /**
    * Commits the acknowledgements, failing with the first error. See [[commitWithResults]].
    */
  def commit(acknowledgements: Chunk[ShareRequest.Ack], sync: Boolean): F[Unit] =
    commitWithResults(acknowledgements, sync).flatMap(firstError(_).traverse_(F.raiseError[Unit]))

  /**
    * Commits the acknowledgements, completing with the result per partition. Fails when the whole
    * request fails or the actor ends first.
    */
  def commitWithResults(
    acknowledgements: Chunk[ShareRequest.Ack],
    sync: Boolean
  ): F[CommitResult] =
    if (acknowledgements.isEmpty) F.pure(Map.empty)
    else
      Deferred[F, Either[Throwable, CommitResult]].flatMap { done =>
        requests.offer(ShareRequest.Commit(acknowledgements, sync, done.complete(_).void)) *>
          F.race(finished.get, done.get)
            .flatMap {
              case Right(result)     => result.liftTo[F]
              case Left(termination) =>
                val ended = termination.fold(identity, _ => ConsumerShutdownException())
                done.tryGet.flatMap(_.fold(F.raiseError[CommitResult](ended))(_.liftTo[F]))
            }
      }

  /**
    * Emits the records, each with an action marking it as handed out, so a merge of several
    * consumers marks records only once they leave it.
    */
  def entries: Stream[F, (AcknowledgeableConsumerRecord[F, K, V], F[Unit])] =
    Stream
      .eval(readerStarted.getAndSet(true))
      .flatMap {
        case true  => Stream.raiseError[F](StreamAlreadyStartedException())
        case false =>
          val failed = finished
            .get
            .flatMap(_.fold(e => F.pure(e.asLeft[Unit]), _ => F.never[Either[Throwable, Unit]]))
          Stream.exec(requests.offer(ShareRequest.Start())) ++
            Stream
              .fromQueueNoneTerminated(output)
              .flatMap(Stream.chunk)
              .map(entry =>
                entry.record -> (entry.emitted.set(true) *> requests
                  .offer(ShareRequest.HandedOut())
                  .whenA(implicitMode))
              )
              .onFinalize(requests.offer(ShareRequest.ReaderClosed()))
              .interruptWhen(failed)
      }

  def stopConsuming: F[Unit] = requests.offer(ShareRequest.Stop())

  def awaitTermination: F[Unit] = finished.get.rethrow

  def terminate: F[Unit] = shutdown.complete(()) *> finished.get.void

  private def receive(state: State, request: ShareRequest[F]): F[State] =
    request match {
      case ShareRequest.Start() if state.started => F.pure(state)
      case ShareRequest.Start()                  =>
        withConsumer
          .blocking(_.subscription().isEmpty)
          .ifM(F.raiseError(NotSubscribedException()), F.pure(state.withStarted))
      case ShareRequest.Stop()                   => F.pure(state.withStopping)
      case ShareRequest.HandedOut()              => F.pure(state)
      case ShareRequest.ReaderClosed()           => F.pure(state.withReaderClosed)
      case commit @ ShareRequest.Commit(_, _, _) => F.pure(state.withCommit(commit))
    }

  private def awaitRequest(state: State): F[State] =
    requests.take.flatMap(receive(state, _))

  private def awaitRequestOrTimeout(state: State, timeout: Option[FiniteDuration]): F[State] =
    timeout.fold(awaitRequest(state)) { duration =>
      takeWithin(duration).flatMap(_.fold(F.pure(state))(receive(state, _)))
    }

  /**
    * Takes a request, or `None` after the duration. A plain race would drop a request taken just as
    * the timer wins, so a taker which lost is cancelled and its request kept, if it got one.
    */
  private def takeWithin(duration: FiniteDuration): F[Option[ShareRequest[F]]] =
    F.racePair(requests.take, F.sleep(duration))
      .flatMap {
        case Left((taken, timer)) =>
          timer.cancel *> taken.embedNever.map(Some(_))
        case Right((taker, _)) =>
          taker.cancel *> taker
            .join
            .flatMap {
              case Outcome.Succeeded(request) => request.map(Some(_))
              case Outcome.Errored(error)     => F.raiseError(error)
              case Outcome.Canceled()         => F.pure(None)
            }
      }

  private def loop(state: State): F[Unit] =
    pendingRef.set(state.pending) *> requests
      .tryTakeN(None)
      .flatMap(_.foldM(state)(receive))
      .flatMap(confirm)
      .flatMap(state => if (state.isFinished) F.unit else step(state).flatMap(loop))

  private def step(state: State): F[State] =
    if (implicitMode) implicitStep(state) else explicitStep(state)

  /**
    * Steps the actor in implicit acknowledgement mode, where Java accepts a poll's records on the
    * next poll or commit, and commit requests fail.
    *
    * The actor polls again once the previous poll is handed out, so records may still be processing
    * when accepted. After stopping, the last records are accepted once handed out. If the stream
    * ends first, closing the consumer releases them.
    */
  private def implicitStep(state: State): F[State] =
    if (state.commits.nonEmpty)
      state
        .commits
        .traverse_(_.callback(Left(ImplicitAcknowledgementException())))
        .as(state.copy(commits = Vector.empty))
    else
      state
        .inFlight
        .values
        .toVector
        .forallM(handedOut)
        .flatMap { allHandedOut =>
          if (state.readerClosed)
            if (allHandedOut && state.inFlight.nonEmpty) acceptImplicitly(state)
            else F.pure(state.copy(inFlight = Map.empty))
          else if (!state.started || !allHandedOut) awaitRequest(state)
          else if (state.admitting) poll(state)
          else if (state.inFlight.nonEmpty) acceptImplicitly(state)
          else awaitRequest(state)
        }

  private def handedOut(record: InFlightRecord[F]): F[Boolean] =
    record.emitted.fold(F.pure(false))(_.get)

  // In implicit acknowledgement mode, a commit accepts the records of the latest poll.
  private def acceptImplicitly(state: State): F[State] =
    effectiveCommitTimeout(settings, state.lock)
      .liftTo[F]
      .flatMap(client.commitSync(Vector.empty, _))
      .flatMap(firstError(_).traverse_(F.raiseError[Unit]))
      .as(state.copy(inFlight = Map.empty))

  private def explicitStep(state: State): F[State] =
    F.monotonic
      .flatMap(now => expire(state, now).tupleRight(now))
      .flatMap { case (state, now) =>
        val renewal      = nextRenewal(state, now)
        val renewalFirst = !(state.renewedLast && state.commits.nonEmpty)
        renewal match {
          case Some(at) if at <= now && renewalFirst =>
            renew(state).map(_.copy(renewedLast = true))
          case _ if state.commits.nonEmpty && state.sent.size < maxSentRounds =>
            processCommits(state).map(_.copy(renewedLast = false))
          case _ =>
            unemitted(state).flatMap { toRelease =>
              if (toRelease.nonEmpty) release(state, toRelease)
              else if (state.canPoll) poll(state)
              else if (state.sent.nonEmpty) awaitResults(state, renewal, now)
              else awaitRequestOrTimeout(state, renewal.map(_ - now))
            }
        }
      }

  /**
    * Waits for a request, or else asks Java for the results of the sent rounds, as Java reports
    * them only during consumer calls. The wait grows while no results arrive.
    */
  private def awaitResults(
    state: State,
    renewal: Option[FiniteDuration],
    now: FiniteDuration
  ): F[State] = {
    val wakeUp = (state.nudgeBackoff :: (state.sent.map(_.deadline) ++ renewal).map(_ - now).toList)
      .min
      .max(Duration.Zero)
    val nudge = client
      .nudge
      .as(state.copy(nudgeBackoff = (state.nudgeBackoff * 2).min(settings.pollInterval)))
    // Commits waiting only for room among the sent rounds would win every race, so the results
    // which make room would never be asked for.
    if (state.commits.nonEmpty) F.sleep(wakeUp) *> nudge
    else takeWithin(wakeUp).flatMap(_.fold(nudge)(receive(state, _)))
  }

  /**
    * A decision waiting for a renewed record needs the next poll, which Java allows only once the
    * other records are acknowledged or renewed. So the actor renews them, even without automatic
    * renewal: at once when the broker confirmed the renewal, so the poll returns the record, and
    * otherwise at the renewal interval, until the renewal resolves.
    */
  private def nextRenewal(state: State, now: FiniteDuration): Option[FiniteDuration] =
    if (state.awaitsConfirmedRenewal && state.commits.isEmpty && state.unacknowledged.nonEmpty)
      Some(now)
    else if (settings.autoRenewal || state.deferred.nonEmpty) state.nextRenewal
    else None

  private def unemitted(state: State): F[Vector[ShareState.Id]] =
    state
      .unacknowledged
      .toVector
      .filterA { case (_, record) =>
        record
          .emitted
          .fold(F.pure(true)) { emitted =>
            if (state.readerClosed) emitted.get.map(!_) else F.pure(false)
          }
      }
      .map(_.map(_._1))

  private def release(state: State, ids: Vector[ShareState.Id]): F[State] =
    internalCommit(state, ids, AcknowledgeType.Release)

  private def renew(state: State): F[State] =
    internalCommit(state, state.unacknowledged.keys.toVector, AcknowledgeType.Renew)

  private def internalCommit(
    state: State,
    ids: Vector[ShareState.Id],
    acknowledgeType: AcknowledgeType
  ): F[State] = {
    val staged = ids.flatMap(id =>
      state
        .inFlight
        .get(id)
        .map(record =>
          ShareRequest.Ack(id._1, id._2, Some(record.deliveryCount), acknowledgeType) -> record
        )
    )
    // Records whose renewal or release failed are dropped, so their lock expires, and a timed-out
    // renewal is kept; only a failure to send fails the stream.
    sendRound(
      state,
      Vector(Part(staged, (_: Either[Throwable, CommitResult]) => F.unit)),
      viaPoll = false
    ).flatMap {
      case (next, None)     => F.pure(next)
      case (_, Some(error)) => F.raiseError[State](error)
    }
  }

  /**
    * Commits one round: the first request if it is synchronous, or else the asynchronous requests
    * up to the next synchronous one. Each acknowledgement is staged, deferred while its record is
    * renewing, or failed with a [[RecordNotInFlightException]]. As in Java, the last decision for a
    * record in a round wins; the requests whose decisions it replaced get the result of its
    * partition.
    */
  private def processCommits(state: State): F[State] = {
    val (commits, rest) =
      if (state.commits.headOption.exists(_.sync)) state.commits.splitAt(1)
      else state.commits.span(!_.sync)

    commits
      .foldLeftM(Round[F]())((round, commit) => addToRound(state, round, commit))
      .flatMap { round =>
        val next =
          state.copy(commits = rest, deferred = state.deferred ++ round.deferred)
        if (round.staged.isEmpty) F.pure(next)
        else if (commits.headOption.exists(_.sync)) commitSyncRound(next, round.staged)
        else sendRound(next, round.staged, viaPoll = ridesWithPoll(next, round.staged)).map(_._1)
      }
  }

  /**
    * Whether the acknowledgements of a round can go with the next poll, as Java sends them, instead
    * of in a request of their own: they settle the latest poll, nothing else waits, and the latest
    * poll was full. On a quiet topic, the broker may hold a fetch while it waits for records, which
    * would delay the confirmation, so they are committed on their own.
    */
  private def ridesWithPoll(state: State, parts: Vector[Part[F]]): Boolean = {
    val staged = parts.flatMap(_.staged)
    val ids    = staged.iterator.map(_._1.id).toSet
    state.admitting && state.commits.isEmpty && state.lastPollRecords >= maxPollRecords &&
    staged.forall(_._1.acknowledgeType != AcknowledgeType.Renew) &&
    state.unacknowledged.keysIterator.forall(ids.contains)
  }

  private def addToRound(
    state: State,
    round: Round[F],
    commit: ShareRequest.Commit[F]
  ): F[Round[F]] = {
    val parts = split(state, commit.acknowledgements)
    // The last decision for a record in a round wins, as in Java, replacing earlier ones.
    val decided  = parts.staged.map(_._1.id).toSet
    val replaced = decided.filter(round.claimed.contains)
    val earlier  =
      if (replaced.isEmpty) round.staged
      else
        round
          .staged
          .map(part => part.copy(staged = part.staged.filterNot(s => replaced.contains(s._1.id))))
    def retry(acks: Vector[ShareRequest.Ack], callback: Callback[F]) =
      ShareRequest.Commit[F](Chunk.from(acks), commit.sync, callback)

    joinCallbacks(commit.callback, parts.count).flatMap { callbacks =>
      val next     = callbacks.iterator
      val staged   = when(parts.staged.nonEmpty)(Part(parts.staged, next.next()))
      val deferred = when(parts.deferred.nonEmpty)(retry(parts.deferred, next.next()))
      val invalid  = when(parts.invalid.nonEmpty)(next.next())
      invalid
        .traverse_(_(Right(notInFlight(parts.invalid))))
        .as(Round(round.claimed ++ decided, earlier ++ staged, round.deferred ++ deferred))
    }
  }

  private def split(state: State, acknowledgements: Chunk[ShareRequest.Ack]): Split[F] = {
    val split = acknowledgements.foldLeft(Split[F]()) { (split, ack) =>
      val record =
        state.inFlight.get(ack.id).filter(r => ack.deliveryCount.forall(_ == r.deliveryCount))
      record match {
        case Some(r) if r.renewing => split.copy(deferred = split.deferred :+ ack)
        case Some(r)               => split.copy(staged = split.staged :+ (ack -> r))
        case None                  => split.copy(invalid = split.invalid :+ ack)
      }
    }
    split.copy(staged = lastDecisions(split.staged))
  }

  /**
    * Sends a round, with `commitAsync` or with the next poll, without waiting for its results,
    * which [[confirm]] applies later. Returns the failure if the round could not be sent, in which
    * case it is already completed with it.
    */
  private def sendRound(
    state: State,
    parts: Vector[Part[F]],
    viaPoll: Boolean
  ): F[(State, Option[Throwable])] = {
    val staged = parts.flatMap(_.staged)
    for {
      timeout <- effectiveCommitTimeout(settings, state.lock).liftTo[F]
      start   <- F.monotonic
      sent    <-
        (if (viaPoll) client.pollWithAcknowledgements(staged, settings.pollTimeout)
         else client.commitAsync(staged).as(None)).attempt
      marked = markSent(state, staged, start)
      next  <- sent match {
                case Left(error) =>
                  confirmRound(
                    marked,
                    SentRound(parts, Set.empty, Map.empty, start, timeout),
                    Left(error)
                  ).map(_ -> Some(error))
                case Right(records) =>
                  val round     = SentRound(parts, staged.map(_._1.id).toSet, Map.empty, start, timeout)
                  val withRound =
                    marked.copy(sent = marked.sent :+ round, nudgeBackoff = ShareState.initialNudge)
                  records
                    .fold(F.pure(withRound)) { case (polledRecords, lock) =>
                      polled(withRound, start, polledRecords, lock.orElse(withRound.lock))
                    }
                    .map(_ -> None)
              }
    } yield next
  }

  // Commits a synchronous round with Java's `commitSync`, which waits for its results.
  private def commitSyncRound(state: State, parts: Vector[Part[F]]): F[State] = {
    val staged = parts.flatMap(_.staged)
    for {
      timeout     <- effectiveCommitTimeout(settings, state.lock).liftTo[F]
      submittedAt <- F.monotonic
      result      <- client.commitSync(staged, timeout).attempt
      round        = SentRound(parts, Set.empty, Map.empty, submittedAt, timeout)
      next        <- confirmRound(markSent(state, staged, submittedAt), round, result)
    } yield next
  }

  /**
    * Records with a final decision stop being tracked once it is sent, so later decisions for them
    * fail. Renewed records wait for the renewal's result, as their lock may still be extended.
    */
  private def markSent(
    state: State,
    staged: Vector[Staged[F]],
    submittedAt: FiniteDuration
  ): State =
    staged.foldLeft(state) { case (s, (ack, record)) =>
      if (ack.acknowledgeType == AcknowledgeType.Renew)
        s.copy(inFlight =
          s.inFlight
            .updated(
              ack.id,
              record.copy(renewing = true, lockedSince = submittedAt, renewalConfirmed = false)
            )
        )
      else s.copy(inFlight = s.inFlight - ack.id)
    }

  /**
    * Applies the results Java reported to the sent rounds, oldest first, and completes the rounds
    * whose results are all known.
    */
  private def confirm(state: State): F[State] =
    client
      .drain
      .flatMap { reported =>
        if (reported.isEmpty) F.pure(state)
        else {
          val sent = reported.foldLeft(state.sent) { case (rounds, (partition, offsets, error)) =>
            offsets.foldLeft(rounds) { (rounds, offset) =>
              val id = partition -> offset
              rounds.indexWhere(_.remaining.contains(id)) match {
                case -1    => rounds
                case index =>
                  val round = rounds(index)
                  rounds.updated(
                    index,
                    round.copy(
                      remaining = round.remaining - id,
                      results = mergeResults(round.results, Map(partition -> error))
                    )
                  )
              }
            }
          }
          val (done, open) = sent.partition(_.remaining.isEmpty)
          done.foldLeftM(state.copy(sent = open, nudgeBackoff = ShareState.initialNudge)) {
            (s, round) => confirmRound(s, round, Right(round.results))
          }
        }
      }

  // Fails the sent rounds whose results did not all arrive within the commit timeout.
  private def expire(state: State, now: FiniteDuration): F[State] = {
    val (expired, open) = state.sent.partition(_.deadline <= now)
    expired.foldLeftM(state.copy(sent = open)) { (s, round) =>
      confirmRound(
        s,
        round,
        Left(ShareCommitTimeoutException(round.timeout, offsetsOf(round.staged, None)))
      )
    }
  }

  /**
    * Completes a round with its result: a renewal is confirmed, kept unconfirmed if it timed out,
    * or else its record is dropped, so its lock expires. Each callback gets its partitions'
    * results.
    */
  private def confirmRound(
    state: State,
    round: SentRound[F],
    result: Either[Throwable, CommitResult]
  ): F[State] = {
    val next = round
      .staged
      .foldLeft(state) { case (s, (ack, record)) =>
        val renewing = s
          .inFlight
          .get(ack.id)
          .filter(r => r.renewing && r.deliveryCount == record.deliveryCount)
        (ack.acknowledgeType, renewing) match {
          case (AcknowledgeType.Renew, Some(r)) =>
            failure(ack, result) match {
              case None =>
                s.copy(inFlight = s.inFlight.updated(ack.id, r.copy(renewalConfirmed = true)))
              case Some(_: TimeoutException) => s
              case Some(_)                   => s.copy(inFlight = s.inFlight - ack.id)
            }
          case _ => s
        }
      }
    pendingRef.set(next.pending) *>
      round
        .parts
        .traverse_ { part =>
          part.callback(
            result.map(results => part.partitions.map(p => p -> results.get(p).flatten).toMap)
          )
        }
        .as(next)
  }

  private def failure(
    ack: ShareRequest.Ack,
    result: Either[Throwable, CommitResult]
  ): Option[Throwable] =
    result.fold(Some(_), _.get(ack.topicPartition).flatten)

  /**
    * Polls once, tracks new and renewed records, and retries deferred commits.
    */
  private def poll(state: State): F[State] =
    F.monotonic
      .flatMap { start =>
        withConsumer
          .blocking { consumer =>
            if (consumer.subscription().isEmpty) None
            else {
              val records = consumer.poll(settings.pollTimeout.toJava)
              Some(
                records.iterator().asScala.toVector -> acquisitionLock(consumer).orElse(state.lock)
              )
            }
          }
          .flatMap {
            case None                  => F.sleep(settings.pollInterval).as(state)
            case Some((records, lock)) => polled(state, start, records, lock)
          }
      }

  private def polled(
    state: State,
    start: FiniteDuration,
    records: Vector[KafkaByteConsumerRecord],
    lock: Option[FiniteDuration]
  ): F[State] = {
    val identified: Vector[((TopicPartition, Long), KafkaByteConsumerRecord)] = records.map(
      record => (new TopicPartition(record.topic, record.partition) -> record.offset) -> record
    )
    // Any record the actor did not renew is a new delivery.
    def renewed(id: ShareState.Id): Option[InFlightRecord[F]] =
      state.inFlight.get(id).filter(_.renewing)
    val admitted = identified.count { case (id, _) => renewed(id).isEmpty }
    // Records may have been acquired by an earlier poll's fetch.
    val acquiredAt =
      if (state.lastPollRecords == 0) state.lastPollStart
      else if (batchOptimized && state.lastPollRecords >= maxPollRecords) state.lastAcquiredAt
      else start
    // Renewing records the poll did not return lost their lock, unless their renewal timed out.
    val unresolved = state
      .inFlight
      .filter { case (_, record) =>
        record.renewing && !record.renewalConfirmed && admitted == 0 &&
        start < record.lockedSince + lock.fold(apiTimeout)(_.max(apiTimeout))
      }
    for {
      renewAfter <- renewalInterval(lock)
      tracked    <-
        identified.foldLeftM((unresolved, Vector.empty[Entry[F, K, V]])) {
          case ((inFlight, entries), (id, record)) =>
            renewed(id) match {
              case Some(again) =>
                F.pure(
                  inFlight.updated(id, again.copy(renewing = false, renewalConfirmed = true)) ->
                    entries
                )
              case None =>
                admit(state, record, acquiredAt).map { case (admitted, entry) =>
                  (inFlight.updated(id, admitted), entries ++ entry)
                }
            }
        }
      (inFlight, entries) = tracked
      next                = state.copy(
               inFlight = inFlight,
               commits = state.deferred ++ state.commits,
               deferred = Vector.empty,
               lock = lock,
               renewAfter = renewAfter,
               lastPollStart = start,
               lastPollRecords = records.size,
               lastAcquiredAt = acquiredAt
             )
      // Updated before the records are handed out, so committers see the new count.
      _ <- pendingRef.set(next.pending)
      _ <- output.offer(Some(Chunk.from(entries))).whenA(entries.nonEmpty)
    } yield next
  }

  private def admit(
    state: State,
    record: KafkaByteConsumerRecord,
    acquiredAt: FiniteDuration
  ): F[(InFlightRecord[F], Option[Entry[F, K, V]])] = {
    val deliveryCount: Short =
      if (record.deliveryCount().isPresent) record.deliveryCount().get.shortValue else 1
    def inFlight(emitted: Option[Ref[F, Boolean]]) =
      InFlightRecord[F](record, deliveryCount, emitted, renewing = false, acquiredAt)

    if (!state.admitting) F.pure(inFlight(None) -> None)
    else
      for {
        decoded  <- ConsumerRecord.fromJava(record, keyDeserializer, valueDeserializer)
        emitted  <- Ref.of[F, Boolean](false)
        partition = new TopicPartition(record.topic, record.partition)
        offset    = AcknowledgeableOffset(partition, record.offset, deliveryCount, committer)
      } yield inFlight(Some(emitted)) ->
        Some(Entry(AcknowledgeableConsumerRecord(decoded, offset), emitted))
  }

  /**
    * Renews early enough that a renewal behind one commit still beats the lock and
    * `max.poll.interval.ms`, or at the poll interval without a known lock.
    */
  private def renewalInterval(lock: Option[FiniteDuration]): F[FiniteDuration] =
    lock match {
      case None           => F.pure(settings.pollInterval.min(maxPollInterval / 2))
      case Some(duration) =>
        effectiveCommitTimeout(settings, lock)
          .liftTo[F]
          .map(timeout =>
            (duration / 2).min(latestRenewal(duration, timeout)).min(maxPollInterval / 2)
          )
    }

  private[actor] def run: F[Unit] =
    F.monotonic
      .flatMap { now =>
        F.race(
          shutdown.get,
          client.install *>
            loop(ShareState.empty[F](now, settings.pollInterval.min(maxPollInterval / 2)))
        )
      }
      .attempt
      .flatMap {
        case Right(_)    => output.offer(None) *> finished.complete(Right(())).void
        case Left(error) => finished.complete(Left(error)).void
      }

}

private[kafka] object KafkaShareConsumerActor {

  // The most rounds of commit requests sent to Java and awaiting their results at once.
  private[actor] val maxSentRounds: Int = 4

  final private[actor] case class Entry[F[_], K, V](
    record: AcknowledgeableConsumerRecord[F, K, V],
    emitted: Ref[F, Boolean]
  )

  /**
    * Creates the actor from the settings, reading the Java configuration it needs.
    */
  def resource[F[_], K, V](
    settings: ShareConsumerSettings[F, K, V],
    keyDeserializer: KeyDeserializer[F, K],
    valueDeserializer: ValueDeserializer[F, V],
    withConsumer: WithShareConsumer[F]
  )(implicit F: Async[F]): Resource[F, KafkaShareConsumerActor[F, K, V]] = {
    def property[A](key: String, default: A)(parse: String => A): F[A] =
      F.delay(settings.properties.get(key).fold(default)(parse))

    for {
      maxPollInterval <- Resource.eval(
                           property(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 300000L)(_.toLong)
                         )
      apiTimeout <- Resource.eval(
                      property(ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, 60000L)(_.toLong)
                    )
      implicitMode <- Resource.eval(
                        property(ConsumerConfig.SHARE_ACKNOWLEDGEMENT_MODE_CONFIG, true)(
                          _.equalsIgnoreCase("implicit")
                        )
                      )
      batchOptimized <- Resource.eval(
                          property(ConsumerConfig.SHARE_ACQUIRE_MODE_CONFIG, true)(
                            _.equalsIgnoreCase("batch_optimized")
                          )
                        )
      maxPollRecords <- Resource.eval(
                          property(
                            ConsumerConfig.MAX_POLL_RECORDS_CONFIG,
                            ConsumerConfig.DEFAULT_MAX_POLL_RECORDS
                          )(_.toInt)
                        )
      requests      <- Resource.eval(Queue.unbounded[F, ShareRequest[F]])
      output        <- Resource.eval(Queue.unbounded[F, Option[Chunk[Entry[F, K, V]]]])
      pending       <- Resource.eval(Ref.of[F, Int](0))
      readerStarted <- Resource.eval(Ref.of[F, Boolean](false))
      shutdown      <- Resource.eval(Deferred[F, Unit])
      finished      <- Resource.eval(Deferred[F, Either[Throwable, Unit]])
      // Java does not allow renewal in implicit acknowledgement mode.
      actor = new KafkaShareConsumerActor(
                settings = if (implicitMode) settings.withAutoRenewal(false) else settings,
                keyDeserializer = keyDeserializer,
                valueDeserializer = valueDeserializer,
                withConsumer = withConsumer,
                requests = requests,
                output = output,
                pendingRef = pending,
                readerStarted = readerStarted,
                shutdown = shutdown,
                finished = finished,
                maxPollInterval = maxPollInterval.millis,
                apiTimeout = apiTimeout.millis,
                implicitMode = implicitMode,
                batchOptimized = batchOptimized,
                maxPollRecords = maxPollRecords
              )
      _ <- Resource.make(F.start(actor.run))(fiber =>
             fiber.cancel *> finished.complete(Left(ConsumerShutdownException())).void
           )
    } yield actor
  }

}
