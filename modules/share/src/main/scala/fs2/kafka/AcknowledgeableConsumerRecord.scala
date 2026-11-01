/*
 * Copyright 2018 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka

import cats.{Applicative, Bitraverse, Eq, Eval, Show, Traverse}
import cats.~>
import cats.syntax.bifoldable.*
import cats.syntax.bitraverse.*
import cats.syntax.eq.*
import cats.syntax.foldable.*
import cats.syntax.functor.*
import cats.syntax.show.*
import cats.syntax.traverse.*

/**
  * [[AcknowledgeableConsumerRecord]] is a Kafka record along with an instance of
  * [[AcknowledgeableOffset]], which stages a decision for the record in its consumer. In explicit
  * acknowledgement mode, the default in this library, every record of a poll needs a decision
  * before its consumer can poll new records; the next poll sends the decisions. Decisions for many
  * records can be staged together with [[acknowledgeChunks]].
  *
  * In implicit acknowledgement mode, records cannot be acknowledged. Instead, the consumer polls
  * again once every record of the previous poll has been emitted by the stream, and that poll
  * accepts them, even if they are still being processed. A record whose processing fails is
  * therefore not delivered again (at-most-once). See the share consumers documentation for details.
  *
  * Experimental: this API targets Kafka's share groups and may change in minor releases.
  */
sealed abstract class AcknowledgeableConsumerRecord[F[_], +K, +V] { self =>

  /**
    * The Kafka record for the [[AcknowledgeableConsumerRecord]]. In implicit acknowledgement mode,
    * simply use this to get the [[ConsumerRecord]] and discard the [[offset]]. In explicit
    * acknowledgement mode, every record needs to be acknowledged through its [[offset]].
    */
  def record: ConsumerRecord[K, V]

  /**
    * An [[AcknowledgeableOffset]] instance, providing a way to acknowledge the [[record]].
    */
  def offset: AcknowledgeableOffset[F]

  /**
    * Creates a new [[AcknowledgeableConsumerRecord]] in which the effect type has been changed
    * using the specified `FunctionK`.
    */
  final def mapK[G[_]](f: F ~> G): AcknowledgeableConsumerRecord[G, K, V] =
    AcknowledgeableConsumerRecord(self.record, self.offset.mapK(f))

}

object AcknowledgeableConsumerRecord {

  final private[this] class AcknowledgeableConsumerRecordImpl[F[_], +K, +V](
    override val record: ConsumerRecord[K, V],
    override val offset: AcknowledgeableOffset[F]
  ) extends AcknowledgeableConsumerRecord[F, K, V] {

    override def toString: String =
      s"AcknowledgeableConsumerRecord($record, $offset)"

  }

  /**
    * Creates a new [[AcknowledgeableConsumerRecord]] using the specified [[ConsumerRecord]] and
    * [[AcknowledgeableOffset]].
    */
  def apply[F[_], K, V](
    record: ConsumerRecord[K, V],
    offset: AcknowledgeableOffset[F]
  ): AcknowledgeableConsumerRecord[F, K, V] =
    new AcknowledgeableConsumerRecordImpl(record, offset)

  def unapply[F[_], K, V](
    acknowledgeable: AcknowledgeableConsumerRecord[F, K, V]
  ): Some[(ConsumerRecord[K, V], AcknowledgeableOffset[F])] =
    Some((acknowledgeable.record, acknowledgeable.offset))

  implicit def acknowledgeableConsumerRecordShow[F[_], K: Show, V: Show]
    : Show[AcknowledgeableConsumerRecord[F, K, V]] = Show.show { cm =>
    show"AcknowledgeableConsumerRecord(${cm.record}, ${cm.offset})"
  }

  implicit def acknowledgeableConsumerRecordEq[F[_], K: Eq, V: Eq]
    : Eq[AcknowledgeableConsumerRecord[F, K, V]] =
    Eq.instance { case (l, r) =>
      l.record === r.record && l.offset === r.offset
    }

  implicit def acknowledgeableConsumerRecordBitraverse[F[_]]
    : Bitraverse[AcknowledgeableConsumerRecord[F, *, *]] =
    new Bitraverse[AcknowledgeableConsumerRecord[F, *, *]] {

      override def bitraverse[G[_], A, B, C, D](
        fab: AcknowledgeableConsumerRecord[F, A, B]
      )(f: A => G[C], g: B => G[D])(implicit
        G: Applicative[G]
      ): G[AcknowledgeableConsumerRecord[F, C, D]] =
        fab
          .record
          .bitraverse(f, g)
          .map { (cd: ConsumerRecord[C, D]) =>
            AcknowledgeableConsumerRecord(cd, fab.offset)
          }

      override def bifoldLeft[A, B, C](
        fab: AcknowledgeableConsumerRecord[F, A, B],
        c: C
      )(f: (C, A) => C, g: (C, B) => C): C =
        fab.record.bifoldLeft(c)(f, g)

      override def bifoldRight[A, B, C](
        fab: AcknowledgeableConsumerRecord[F, A, B],
        c: Eval[C]
      )(f: (A, Eval[C]) => Eval[C], g: (B, Eval[C]) => Eval[C]): Eval[C] =
        fab.record.bifoldRight(c)(f, g)

    }

  implicit def acknowledgeableConsumerRecordTraverse[F[_], K]
    : Traverse[AcknowledgeableConsumerRecord[F, K, *]] =
    new Traverse[AcknowledgeableConsumerRecord[F, K, *]] {

      override def traverse[G[_], A, B](
        fa: AcknowledgeableConsumerRecord[F, K, A]
      )(f: A => G[B])(implicit G: Applicative[G]): G[AcknowledgeableConsumerRecord[F, K, B]] =
        fa.record
          .traverse(f)
          .map { (b: ConsumerRecord[K, B]) =>
            AcknowledgeableConsumerRecord(b, fa.offset)
          }

      override def foldLeft[A, B](fa: AcknowledgeableConsumerRecord[F, K, A], b: B)(
        f: (B, A) => B
      ): B =
        fa.record.foldLeft(b)(f)

      override def foldRight[A, B](
        fa: AcknowledgeableConsumerRecord[F, K, A],
        lb: Eval[B]
      )(f: (A, Eval[B]) => Eval[B]): Eval[B] =
        fa.record.foldRight(lb)(f)

    }

}
