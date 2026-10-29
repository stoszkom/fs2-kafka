/*
 * Copyright 2018 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka

import java.util.concurrent.TimeUnit
import java.util.UUID

import scala.concurrent.duration.*

import cats.effect.unsafe.implicits.global
import cats.effect.IO
import fs2.kafka.internal.converters.collection.*

import org.apache.kafka.clients.admin.{
  AlterConfigOp,
  ConfigEntry,
  FeatureUpdate,
  UpdateFeaturesOptions
}
import org.apache.kafka.common.config.ConfigResource

abstract class ShareConsumerBaseKafkaSpec extends BaseKafkaSpec {

  // Kafka 4.3, for share groups with lock renewal.
  override protected def imageVersion: String = "8.3.2"

  // The shortest lock the broker allows, to keep the lock tests short.
  protected lazy val lock: FiniteDuration = 15.seconds

  override protected def brokerEnvironment: Map[String, String] =
    Map(
      "KAFKA_SHARE_COORDINATOR_STATE_TOPIC_REPLICATION_FACTOR" -> "1",
      "KAFKA_SHARE_COORDINATOR_STATE_TOPIC_MIN_ISR"            -> "1",
      "KAFKA_GROUP_SHARE_RECORD_LOCK_DURATION_MS"              -> lock.toMillis.toString
    )

  // Share groups are enabled by the share.version feature, which may be off on a new cluster.
  override def afterStart(): Unit =
    withAdminClient { admin =>
      val features = admin.describeFeatures().featureMetadata().get(20, TimeUnit.SECONDS)
      val maxLevel = features.supportedFeatures().asScala("share.version").maxVersion()
      if (
        features
          .finalizedFeatures()
          .asScala
          .get("share.version")
          .forall(_.maxVersionLevel() < maxLevel)
      )
        admin
          .updateFeatures(
            Map("share.version" -> new FeatureUpdate(maxLevel, FeatureUpdate.UpgradeType.UPGRADE))
              .asJava,
            new UpdateFeaturesOptions
          )
          .all()
          .get(20, TimeUnit.SECONDS): Unit
    }.get

  // Settings for a new share group, which reads topics from the beginning.
  protected def shareConsumerSettings: ShareConsumerSettings[IO, String, String] = {
    val group = s"share-group-${UUID.randomUUID()}"
    withAdminClient { admin =>
      val operation =
        new AlterConfigOp(
          new ConfigEntry("share.auto.offset.reset", "earliest"),
          AlterConfigOp.OpType.SET
        )
      admin
        .incrementalAlterConfigs(
          Map(
            new ConfigResource(ConfigResource.Type.GROUP, group) ->
              (List(operation).asJava: java.util.Collection[AlterConfigOp])
          ).asJava
        )
        .all()
        .get(20, TimeUnit.SECONDS)
    }.get
    ShareConsumerSettings[IO, String, String]
      .withBootstrapServers(container.bootstrapServers)
      .withGroupId(group)
      .withCommitTimeout(5.seconds)
  }

  protected def publish(topic: String, values: String*): Unit =
    publishToKafka(topic, values.map(value => value -> value))

  protected def run[A](io: IO[A]): A = io.timeout(2.minutes).unsafeRunSync()

  // Accepts every record up to and including `marker`, returning values and delivery counts.
  protected def readUntil(
    topic: String,
    settings: ShareConsumerSettings[IO, String, String],
    marker: String
  ): Vector[(String, Int)] =
    run(
      KafkaShareConsumer
        .stream(settings)
        .subscribeTo(topic)
        .records
        .evalMap(r =>
          r.offset
            .acknowledge(AcknowledgeType.Accept)
            .commit
            .as(r.record.value -> r.offset.deliveryCount.toInt)
        )
        .takeThrough(_._1 != marker)
        .compile
        .toVector
    )

}
