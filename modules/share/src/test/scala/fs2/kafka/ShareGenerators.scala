/*
 * Copyright 2018 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka

import cats.ApplicativeThrow

import org.scalacheck.{Arbitrary, Cogen, Gen}
import org.scalacheck.rng.Seed

trait ShareGenerators extends BaseGenerators {

  def genKafkaShareCommitter[F[_]: ApplicativeThrow]: Gen[KafkaShareCommitter[F]] =
    Gen.const(
      KafkaShareCommitter[F](
        _ => ApplicativeThrow[F].unit,
        _ => ApplicativeThrow[F].unit,
        _ => ApplicativeThrow[F].pure(Map.empty),
        ApplicativeThrow[F].pure(0)
      )
    )

  def genAcknowledgeableOffset[F[_]: ApplicativeThrow]: Gen[AcknowledgeableOffset[F]] =
    for {
      topicPartition <- genTopicPartition
      offset         <- Gen.posNum[Long]
      deliveryCount  <- Gen.choose[Short](1, 10)
      committer      <- genKafkaShareCommitter[F]
    } yield AcknowledgeableOffset[F](topicPartition, offset, deliveryCount, committer)

  implicit def arbAcknowledgeableOffset[F[_]: ApplicativeThrow]
    : Arbitrary[AcknowledgeableOffset[F]] =
    Arbitrary(genAcknowledgeableOffset[F])

  implicit def cogenAcknowledgeableOffset[F[_]]: Cogen[AcknowledgeableOffset[F]] =
    Cogen { (seed: Seed, offset: AcknowledgeableOffset[F]) =>
      (Cogen
        .perturb(_: Seed, offset.topicPartition))
        .andThen(Cogen.perturb(_, offset.offset))
        .andThen(Cogen.perturb(_, offset.deliveryCount))
        .apply(seed)
    }

  def genAcknowledgeableConsumerRecord[
    F[_]: ApplicativeThrow,
    K: Arbitrary,
    V: Arbitrary
  ]: Gen[AcknowledgeableConsumerRecord[F, K, V]] =
    for {
      record <- Arbitrary.arbitrary[ConsumerRecord[K, V]]
      offset <- genAcknowledgeableOffset[F]
    } yield AcknowledgeableConsumerRecord[F, K, V](record, offset)

  implicit def arbAcknowledgeableConsumerRecord[
    F[_]: ApplicativeThrow,
    K: Arbitrary,
    V: Arbitrary
  ]: Arbitrary[AcknowledgeableConsumerRecord[F, K, V]] =
    Arbitrary(genAcknowledgeableConsumerRecord[F, K, V])

  implicit def cogenAcknowledgeableConsumerRecord[F[_], K: Cogen, V: Cogen]
    : Cogen[AcknowledgeableConsumerRecord[F, K, V]] =
    Cogen { (seed: Seed, record: AcknowledgeableConsumerRecord[F, K, V]) =>
      Cogen.perturbPair(seed, (record.record, record.offset))
    }

}
