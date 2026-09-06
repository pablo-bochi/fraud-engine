package com.fraudengine.detection.adapter.in.kafka;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fraudengine.contracts.CustomerNotificationRequested;
import com.fraudengine.contracts.InternalAlert;
import com.fraudengine.contracts.InvalidEventReference;
import com.fraudengine.contracts.QuarantinedEventReference;
import com.fraudengine.contracts.RuleSetSnapshot;
import com.fraudengine.contracts.TransactionAssessment;
import com.fraudengine.contracts.TransactionEvent;
import com.fraudengine.detection.config.KafkaStreamsConfiguration;
import com.fraudengine.detection.health.StreamsReadinessHealthIndicator;
import java.time.Instant;
import java.util.List;
import java.util.Properties;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.Test;

class DetectionTopologyTest {
  private static final ObjectMapper MAPPER = new ObjectMapper().findAndRegisterModules();

  @Test
  void publishesOnlyANotSuspiciousAssessmentForAnEventBelowTheAmountLimit() {
    Properties properties = new Properties();
    properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "topology-test");
    properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092");

    try (TopologyTestDriver driver =
        new TopologyTestDriver(new DetectionTopology().build(), properties)) {
      TestInputTopic<String, RuleSetSnapshot> rulesets =
          driver.createInputTopic(
              DetectionTopology.RULESET_TOPIC,
              Serdes.String().serializer(),
              new JsonSerde<>(RuleSetSnapshot.class).serializer());
      TestInputTopic<String, TransactionEvent> transactions =
          driver.createInputTopic(
              DetectionTopology.TRANSACTION_TOPIC,
              Serdes.String().serializer(),
              new JsonSerde<>(TransactionEvent.class).serializer());
      TestOutputTopic<String, TransactionAssessment> assessments =
          driver.createOutputTopic(
              DetectionTopology.ASSESSMENT_TOPIC,
              Serdes.String().deserializer(),
              new JsonSerde<>(TransactionAssessment.class).deserializer());

      rulesets.pipeInput("ACTIVE", snapshot());
      transactions.pipeInput("tx-1", event());

      assertThat(assessments.readValue().status()).isEqualTo("NOT_SUSPICIOUS");
      assertThat(
              driver
                  .createOutputTopic(
                      DetectionTopology.ALERT_TOPIC,
                      Serdes.String().deserializer(),
                      new JsonSerde<>(Object.class).deserializer())
                  .isEmpty())
          .isTrue();
    }
  }

  @Test
  void enablesExactlyOnceV2AndReadCommittedForTheEngine() {
    Properties properties = new KafkaStreamsConfiguration().properties("kafka:9092");
    assertThat(properties.getProperty(StreamsConfig.PROCESSING_GUARANTEE_CONFIG))
        .isEqualTo(StreamsConfig.EXACTLY_ONCE_V2);
    assertThat(properties.getProperty("consumer.isolation.level")).isEqualTo("read_committed");
  }

  @Test
  void materializesTheActiveRulesetInAGlobalStore() {
    assertThat(new DetectionTopology().build().describe().toString())
        .contains(DetectionTopology.ACTIVE_RULESETS_GLOBAL_STORE);
  }

  @Test
  void becomesReadyOnlyAfterTheTopologyAcceptsAValidRuleset() {
    Properties properties = new Properties();
    properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "readiness-topology-test");
    properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092");
    StreamsReadinessHealthIndicator readiness = new StreamsReadinessHealthIndicator();
    try (TopologyTestDriver driver =
        new TopologyTestDriver(new DetectionTopology(readiness).build(), properties)) {
      assertThat(readiness.health().getStatus().getCode()).isEqualTo("DOWN");
      driver
          .createInputTopic(
              DetectionTopology.RULESET_TOPIC,
              Serdes.String().serializer(),
              new JsonSerde<>(RuleSetSnapshot.class).serializer())
          .pipeInput("ACTIVE", snapshot());
      assertThat(readiness.health().getStatus().getCode()).isEqualTo("UP");
    }
  }

  @Test
  void publishesAssessmentAlertAndNotificationWithTheSameAlertIdForAMatch() {
    Properties properties = new Properties();
    properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "suspicious-test");
    properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092");
    try (TopologyTestDriver driver =
        new TopologyTestDriver(new DetectionTopology().build(), properties)) {
      var rulesets =
          driver.createInputTopic(
              DetectionTopology.RULESET_TOPIC,
              Serdes.String().serializer(),
              new JsonSerde<>(RuleSetSnapshot.class).serializer());
      var transactions =
          driver.createInputTopic(
              DetectionTopology.TRANSACTION_TOPIC,
              Serdes.String().serializer(),
              new JsonSerde<>(TransactionEvent.class).serializer());
      var alerts =
          driver.createOutputTopic(
              DetectionTopology.ALERT_TOPIC,
              Serdes.String().deserializer(),
              new JsonSerde<>(InternalAlert.class).deserializer());
      var notifications =
          driver.createOutputTopic(
              DetectionTopology.NOTIFICATION_TOPIC,
              Serdes.String().deserializer(),
              new JsonSerde<>(CustomerNotificationRequested.class).deserializer());
      rulesets.pipeInput("ACTIVE", snapshot());
      transactions.pipeInput(
          "tx-2",
          new TransactionEvent(
              1,
              "event-2",
              "tx-2",
              "customer-1",
              10_000,
              "BRL",
              Instant.parse("2026-01-01T00:02:00Z"),
              "PURCHASE",
              "APP",
              "BR",
              "device",
              "trace"));
      InternalAlert alert = alerts.readValue();
      assertThat(notifications.readValue().alertId()).isEqualTo(alert.alertId());
    }
  }

  @Test
  void consolidatesEveryMatchedRuleAndUsesTheHighestSeverity() {
    Properties properties = new Properties();
    properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "multi-match-test");
    properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092");
    try (TopologyTestDriver driver =
        new TopologyTestDriver(new DetectionTopology().build(), properties)) {
      var rulesets =
          driver.createInputTopic(
              DetectionTopology.RULESET_TOPIC,
              Serdes.String().serializer(),
              new JsonSerde<>(RuleSetSnapshot.class).serializer());
      var transactions =
          driver.createInputTopic(
              DetectionTopology.TRANSACTION_TOPIC,
              Serdes.String().serializer(),
              new JsonSerde<>(TransactionEvent.class).serializer());
      var alerts =
          driver.createOutputTopic(
              DetectionTopology.ALERT_TOPIC,
              Serdes.String().deserializer(),
              new JsonSerde<>(InternalAlert.class).deserializer());
      var high = snapshot().rules().getFirst();
      var critical = high.deepCopy();
      ((com.fasterxml.jackson.databind.node.ObjectNode) critical).put("ruleId", "critical");
      ((com.fasterxml.jackson.databind.node.ObjectNode) critical).put("severity", "CRITICAL");
      rulesets.pipeInput(
          "ACTIVE",
          new RuleSetSnapshot(
              1,
              "multi",
              1,
              "multi-hash",
              "multi-v1",
              List.of(high, critical),
              Instant.parse("2026-01-01T00:00:00Z")));
      transactions.pipeInput(
          "multi-tx",
          new TransactionEvent(
              1,
              "multi-event",
              "multi-tx",
              "customer-multi",
              10_000,
              "BRL",
              Instant.parse("2026-01-01T00:02:00Z"),
              "PURCHASE",
              "APP",
              "BR",
              "device",
              "trace"));
      InternalAlert alert = alerts.readValue();
      assertThat(alert.severity()).isEqualTo("CRITICAL");
      assertThat(alert.matchedRules()).hasSize(2);
    }
  }

  @Test
  void suppressesTheExpectedDuplicateAndQuarantinesAConflictingTransactionIdentity() {
    Properties properties = new Properties();
    properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "identity-test");
    properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092");
    try (TopologyTestDriver driver =
        new TopologyTestDriver(new DetectionTopology().build(), properties)) {
      var rulesets =
          driver.createInputTopic(
              DetectionTopology.RULESET_TOPIC,
              Serdes.String().serializer(),
              new JsonSerde<>(RuleSetSnapshot.class).serializer());
      var transactions =
          driver.createInputTopic(
              DetectionTopology.TRANSACTION_TOPIC,
              Serdes.String().serializer(),
              new JsonSerde<>(TransactionEvent.class).serializer());
      var assessments =
          driver.createOutputTopic(
              DetectionTopology.ASSESSMENT_TOPIC,
              Serdes.String().deserializer(),
              new JsonSerde<>(TransactionAssessment.class).deserializer());
      var quarantined =
          driver.createOutputTopic(
              DetectionTopology.QUARANTINE_TOPIC,
              Serdes.String().deserializer(),
              new JsonSerde<>(QuarantinedEventReference.class).deserializer());
      rulesets.pipeInput("ACTIVE", snapshot());
      TransactionEvent original = event();
      transactions.pipeInput("tx-1", original);
      transactions.pipeInput("tx-1", original);
      assertThat(assessments.readKeyValuesToList()).hasSize(1);
      transactions.pipeInput(
          "tx-1",
          new TransactionEvent(
              1,
              "event-other",
              "tx-1",
              "customer-1",
              100,
              "BRL",
              Instant.parse("2026-01-01T00:03:00Z"),
              "PURCHASE",
              "APP",
              "BR",
              "device",
              "trace"));
      assertThat(quarantined.readValue().reasonCode()).isEqualTo("TRANSACTION_IDENTITY_CONFLICT");
      assertThat(assessments.isEmpty()).isTrue();
    }
  }

  @Test
  void keepsTheLastValidRulesetWhenAnOlderOrInvalidUpdateArrives() {
    Properties properties = new Properties();
    properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "ruleset-test");
    properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092");
    try (TopologyTestDriver driver =
        new TopologyTestDriver(new DetectionTopology().build(), properties)) {
      var rulesets =
          driver.createInputTopic(
              DetectionTopology.RULESET_TOPIC,
              Serdes.String().serializer(),
              new JsonSerde<>(RuleSetSnapshot.class).serializer());
      var transactions =
          driver.createInputTopic(
              DetectionTopology.TRANSACTION_TOPIC,
              Serdes.String().serializer(),
              new JsonSerde<>(TransactionEvent.class).serializer());
      var assessments =
          driver.createOutputTopic(
              DetectionTopology.ASSESSMENT_TOPIC,
              Serdes.String().deserializer(),
              new JsonSerde<>(TransactionAssessment.class).deserializer());
      rulesets.pipeInput("ACTIVE", snapshot());
      rulesets.pipeInput(
          "ACTIVE",
          new RuleSetSnapshot(
              1,
              "old",
              0,
              "old-hash",
              "rule-v0",
              snapshot().rules(),
              Instant.parse("2026-01-01T00:00:01Z")));
      rulesets.pipeInput(
          "ACTIVE",
          new RuleSetSnapshot(
              1, "bad", 2, "", "rule-v2", List.of(), Instant.parse("2026-01-01T00:00:02Z")));
      transactions.pipeInput(
          "tx-3",
          new TransactionEvent(
              1,
              "event-3",
              "tx-3",
              "customer-3",
              10_000,
              "BRL",
              Instant.parse("2026-01-01T00:03:00Z"),
              "PURCHASE",
              "APP",
              "BR",
              "device",
              "trace"));
      assertThat(assessments.readValue().rulesetVersion()).isEqualTo(1);
    }
  }

  @Test
  void usesANewerValidRulesetForSubsequentEvaluations() {
    Properties properties = new Properties();
    properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "new-ruleset-test");
    properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092");
    try (TopologyTestDriver driver =
        new TopologyTestDriver(new DetectionTopology().build(), properties)) {
      var rulesets =
          driver.createInputTopic(
              DetectionTopology.RULESET_TOPIC,
              Serdes.String().serializer(),
              new JsonSerde<>(RuleSetSnapshot.class).serializer());
      var transactions =
          driver.createInputTopic(
              DetectionTopology.TRANSACTION_TOPIC,
              Serdes.String().serializer(),
              new JsonSerde<>(TransactionEvent.class).serializer());
      var assessments =
          driver.createOutputTopic(
              DetectionTopology.ASSESSMENT_TOPIC,
              Serdes.String().deserializer(),
              new JsonSerde<>(TransactionAssessment.class).deserializer());
      rulesets.pipeInput("ACTIVE", snapshot());
      var lowerLimit = snapshot().rules().getFirst().deepCopy();
      ((com.fasterxml.jackson.databind.node.ObjectNode) lowerLimit.path("definition"))
          .put("amountMinor", 50);
      rulesets.pipeInput(
          "ACTIVE",
          new RuleSetSnapshot(
              1,
              "snapshot-2",
              2,
              "hash-2",
              "rule-v2",
              List.of(lowerLimit),
              Instant.parse("2026-01-01T00:00:01Z")));
      transactions.pipeInput("tx-1", event());
      TransactionAssessment assessment = assessments.readValue();
      assertThat(assessment.rulesetVersion()).isEqualTo(2);
      assertThat(assessment.status()).isEqualTo("SUSPICIOUS");
    }
  }

  @Test
  void evaluatesCountWindowFromTheCustomerHistoryStore() {
    Properties properties = new Properties();
    properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "history-test");
    properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092");
    try (TopologyTestDriver driver =
        new TopologyTestDriver(new DetectionTopology().build(), properties)) {
      var rulesets =
          driver.createInputTopic(
              DetectionTopology.RULESET_TOPIC,
              Serdes.String().serializer(),
              new JsonSerde<>(RuleSetSnapshot.class).serializer());
      var transactions =
          driver.createInputTopic(
              DetectionTopology.TRANSACTION_TOPIC,
              Serdes.String().serializer(),
              new JsonSerde<>(TransactionEvent.class).serializer());
      var assessments =
          driver.createOutputTopic(
              DetectionTopology.ASSESSMENT_TOPIC,
              Serdes.String().deserializer(),
              new JsonSerde<>(TransactionAssessment.class).deserializer());
      var count =
          JsonNodeFactory.instance
              .objectNode()
              .put("ruleId", "count")
              .put("ruleVersion", "1")
              .put("severity", "HIGH")
              .set(
                  "definition",
                  JsonNodeFactory.instance
                      .objectNode()
                      .put("type", "COUNT_WINDOW")
                      .put("minimumCount", 2)
                      .put("windowSeconds", 600));
      rulesets.pipeInput(
          "ACTIVE",
          new RuleSetSnapshot(
              1,
              "count-snapshot",
              1,
              "count-hash",
              "count-v1",
              List.of(count),
              Instant.parse("2026-01-01T00:00:00Z")));
      transactions.pipeInput(
          "count-1",
          new TransactionEvent(
              1,
              "count-event-1",
              "count-1",
              "customer-count",
              1,
              "BRL",
              Instant.parse("2026-01-01T00:01:00Z"),
              "PURCHASE",
              "APP",
              "BR",
              "device",
              "trace"));
      transactions.pipeInput(
          "count-2",
          new TransactionEvent(
              1,
              "count-event-2",
              "count-2",
              "customer-count",
              1,
              "BRL",
              Instant.parse("2026-01-01T00:02:00Z"),
              "PURCHASE",
              "APP",
              "BR",
              "device",
              "trace"));
      assertThat(assessments.readValue().status()).isEqualTo("NOT_SUSPICIOUS");
      assertThat(assessments.readValue().status()).isEqualTo("SUSPICIOUS");
    }
  }

  @Test
  void usesOnlyFactsAtOrBeforeTheEventTimeAndKeepsLateFactsForFutureEvents() {
    Properties properties = new Properties();
    properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "late-history-test");
    properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092");
    try (TopologyTestDriver driver =
        new TopologyTestDriver(new DetectionTopology().build(), properties)) {
      var rulesets =
          driver.createInputTopic(
              DetectionTopology.RULESET_TOPIC,
              Serdes.String().serializer(),
              new JsonSerde<>(RuleSetSnapshot.class).serializer());
      var transactions =
          driver.createInputTopic(
              DetectionTopology.TRANSACTION_TOPIC,
              Serdes.String().serializer(),
              new JsonSerde<>(TransactionEvent.class).serializer());
      var assessments =
          driver.createOutputTopic(
              DetectionTopology.ASSESSMENT_TOPIC,
              Serdes.String().deserializer(),
              new JsonSerde<>(TransactionAssessment.class).deserializer());
      var count =
          JsonNodeFactory.instance
              .objectNode()
              .put("ruleId", "count")
              .put("ruleVersion", "1")
              .put("severity", "HIGH")
              .set(
                  "definition",
                  JsonNodeFactory.instance
                      .objectNode()
                      .put("type", "COUNT_WINDOW")
                      .put("minimumCount", 3)
                      .put("windowSeconds", 600));
      rulesets.pipeInput(
          "ACTIVE",
          new RuleSetSnapshot(
              1,
              "late-count",
              1,
              "late-hash",
              "count-v1",
              List.of(count),
              Instant.parse("2026-01-01T00:00:00Z")));
      transactions.pipeInput(
          "late-2",
          new TransactionEvent(
              1,
              "late-event-2",
              "late-2",
              "customer-late",
              1,
              "BRL",
              Instant.parse("2026-01-01T00:02:00Z"),
              "PURCHASE",
              "APP",
              "BR",
              "device",
              "trace"));
      transactions.pipeInput(
          "late-1",
          new TransactionEvent(
              1,
              "late-event-1",
              "late-1",
              "customer-late",
              1,
              "BRL",
              Instant.parse("2026-01-01T00:01:00Z"),
              "PURCHASE",
              "APP",
              "BR",
              "device",
              "trace"));
      transactions.pipeInput(
          "late-3",
          new TransactionEvent(
              1,
              "late-event-3",
              "late-3",
              "customer-late",
              1,
              "BRL",
              Instant.parse("2026-01-01T00:03:00Z"),
              "PURCHASE",
              "APP",
              "BR",
              "device",
              "trace"));
      assertThat(assessments.readValue().status()).isEqualTo("NOT_SUSPICIOUS");
      assertThat(assessments.readValue().status()).isEqualTo("NOT_SUSPICIOUS");
      assertThat(assessments.readValue().status()).isEqualTo("SUSPICIOUS");
    }
  }

  @Test
  void marksStatefulEvaluationInconclusiveWhenTheEventIsOlderThanRetainedHistory() {
    Properties properties = new Properties();
    properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "insufficient-history-test");
    properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092");
    try (TopologyTestDriver driver =
        new TopologyTestDriver(new DetectionTopology().build(), properties)) {
      var rulesets =
          driver.createInputTopic(
              DetectionTopology.RULESET_TOPIC,
              Serdes.String().serializer(),
              new JsonSerde<>(RuleSetSnapshot.class).serializer());
      var transactions =
          driver.createInputTopic(
              DetectionTopology.TRANSACTION_TOPIC,
              Serdes.String().serializer(),
              new JsonSerde<>(TransactionEvent.class).serializer());
      var assessments =
          driver.createOutputTopic(
              DetectionTopology.ASSESSMENT_TOPIC,
              Serdes.String().deserializer(),
              new JsonSerde<>(TransactionAssessment.class).deserializer());
      var count =
          JsonNodeFactory.instance
              .objectNode()
              .put("ruleId", "count")
              .put("ruleVersion", "1")
              .put("severity", "HIGH")
              .set(
                  "definition",
                  JsonNodeFactory.instance
                      .objectNode()
                      .put("type", "COUNT_WINDOW")
                      .put("minimumCount", 2)
                      .put("windowSeconds", 600));
      rulesets.pipeInput(
          "ACTIVE",
          new RuleSetSnapshot(
              1,
              "insufficient",
              1,
              "insufficient-hash",
              "count-v1",
              List.of(count),
              Instant.parse("2026-01-01T00:00:00Z")));
      transactions.pipeInput(
          "new",
          new TransactionEvent(
              1,
              "new-event",
              "new",
              "customer-insufficient",
              1,
              "BRL",
              Instant.parse("2026-01-01T01:00:00Z"),
              "PURCHASE",
              "APP",
              "BR",
              "device",
              "trace"));
      transactions.pipeInput(
          "old",
          new TransactionEvent(
              1,
              "old-event",
              "old",
              "customer-insufficient",
              1,
              "BRL",
              Instant.parse("2026-01-01T00:00:00Z"),
              "PURCHASE",
              "APP",
              "BR",
              "device",
              "trace"));
      assessments.readValue();
      TransactionAssessment assessment = assessments.readValue();
      assertThat(assessment.status()).isEqualTo("INCONCLUSIVE");
      assertThat(assessment.ruleResults().getFirst().path("status").asText())
          .isEqualTo("NOT_EVALUATED");
    }
  }

  @Test
  void routesAnInvalidKeyToASanitizedReferenceWithoutBlockingTheNextEvent() {
    Properties properties = new Properties();
    properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "invalid-test");
    properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092");
    try (TopologyTestDriver driver =
        new TopologyTestDriver(new DetectionTopology().build(), properties)) {
      var rulesets =
          driver.createInputTopic(
              DetectionTopology.RULESET_TOPIC,
              Serdes.String().serializer(),
              new JsonSerde<>(RuleSetSnapshot.class).serializer());
      var transactions =
          driver.createInputTopic(
              DetectionTopology.TRANSACTION_TOPIC,
              Serdes.String().serializer(),
              new JsonSerde<>(TransactionEvent.class).serializer());
      var invalid =
          driver.createOutputTopic(
              DetectionTopology.INVALID_TOPIC,
              Serdes.String().deserializer(),
              new JsonSerde<>(InvalidEventReference.class).deserializer());
      var assessments =
          driver.createOutputTopic(
              DetectionTopology.ASSESSMENT_TOPIC,
              Serdes.String().deserializer(),
              new JsonSerde<>(TransactionAssessment.class).deserializer());
      rulesets.pipeInput("ACTIVE", snapshot());
      transactions.pipeInput("wrong-key", event());
      transactions.pipeInput("tx-1", event());
      assertThat(invalid.readValue().reasonCode()).isEqualTo("INVALID_TRANSACTION_EVENT");
      assertThat(assessments.readValue().transactionId()).isEqualTo("tx-1");
    }
  }

  private static RuleSetSnapshot snapshot() {
    return new RuleSetSnapshot(
        1,
        "snapshot-1",
        1,
        "hash-1",
        "rule-v1",
        List.of(
            JsonNodeFactory.instance
                .objectNode()
                .put("ruleId", "amount")
                .put("ruleVersion", "1")
                .put("severity", "HIGH")
                .set(
                    "definition",
                    JsonNodeFactory.instance
                        .objectNode()
                        .put("type", "AMOUNT_THRESHOLD")
                        .put("amountMinor", 10_000)
                        .put("currency", "BRL"))),
        Instant.parse("2026-01-01T00:00:00Z"));
  }

  private static TransactionEvent event() {
    return new TransactionEvent(
        1,
        "event-1",
        "tx-1",
        "customer-1",
        100,
        "BRL",
        Instant.parse("2026-01-01T00:01:00Z"),
        "PURCHASE",
        "APP",
        "BR",
        "device",
        "trace");
  }
}
