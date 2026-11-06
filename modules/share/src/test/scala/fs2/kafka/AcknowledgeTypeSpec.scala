/*
 * Copyright 2018 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka

import org.apache.kafka.clients.consumer.AcknowledgeType as KafkaAcknowledgeType

final class AcknowledgeTypeSpec extends BaseSpec {

  describe("AcknowledgeType") {
    it("should correspond to Java's AcknowledgeType") {
      List(
        AcknowledgeType.Accept,
        AcknowledgeType.Release,
        AcknowledgeType.Reject,
        AcknowledgeType.Renew
      ).map(_.asJava) shouldBe List(
        KafkaAcknowledgeType.ACCEPT,
        KafkaAcknowledgeType.RELEASE,
        KafkaAcknowledgeType.REJECT,
        KafkaAcknowledgeType.RENEW
      )
    }
  }

}
