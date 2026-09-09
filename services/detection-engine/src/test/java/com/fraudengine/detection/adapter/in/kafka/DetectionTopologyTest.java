package com.fraudengine.detection.adapter.in.kafka;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fraudengine.contracts.CustomerNotificationRequested;
import com.fraudengine.contracts.InternalAlert;
import com.fraudengine.contracts.InvalidEventReference;
import com.fraudengine.contracts.QuarantinedEventReference;
import com.fraudengine.contracts.RuleSetSnapshot;
import com.fraudengine.contracts.TransactionAssessment;
import com.fraudengine.contracts.TransactionEvent;
import com.fraudengine.detection.config.KafkaStreamsConfiguration;
import com.fraudengine.detection.health.StreamsReadinessHealthIndicator;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HexFormat;
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

      var assessment = assessments.readKeyValue();
      assertThat(assessment.key).isEqualTo("tx-1");
      assertThat(assessment.value.status()).isEqualTo("NOT_SUSPICIOUS");
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
  void usesTheTopicsProvisionedByTheLocalKafkaInitializer() {
    assertThat(DetectionTopology.TRANSACTION_TOPIC).isEqualTo("fraud.transaction.received.v1");
    assertThat(DetectionTopology.ASSESSMENT_TOPIC).isEqualTo("fraud.assessment.created.v1");
    assertThat(DetectionTopology.ALERT_TOPIC).isEqualTo("fraud.alert.internal.v1");
    assertThat(DetectionTopology.NOTIFICATION_TOPIC).isEqualTo("fraud.notification.requested.v1");
    assertThat(DetectionTopology.QUARANTINE_TOPIC).isEqualTo("fraud.transaction.quarantine.v1");
    assertThat(DetectionTopology.INVALID_TOPIC).isEqualTo("fraud.transaction.invalid.v1");
  }

  @Test
  void repartitionsByCustomerBeforeUsingCustomerScopedStateStores() {
    String description = new DetectionTopology().build().describe().toString();

    assertThat(description).contains("topic: customer-repartition");
    assertThat(description).contains("topics: [customer-repartition]");
  }

  @Test
  void materializesTheAcceptedActiveRulesetInTheGlobalStore() {
    Properties properties = new Properties();
    properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "global-store-test");
    properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092");

    try (TopologyTestDriver driver =
        new TopologyTestDriver(new DetectionTopology().build(), properties)) {

      var rulesets =
          driver.createInputTopic(
              DetectionTopology.RULESET_TOPIC,
              Serdes.String().serializer(),
              new JsonSerde<>(RuleSetSnapshot.class).serializer());

      RuleSetSnapshot snapshot =
          testRuleset(
              "global-store",
              1,
              List.of(testRule("amount", 1, 0, "HIGH", amountCondition(10_000, "BRL"))));

      rulesets.pipeInput("ACTIVE", snapshot);

      var store =
          driver.<String, RuleSetSnapshot>getKeyValueStore(
              DetectionTopology.ACTIVE_RULESETS_GLOBAL_STORE);

      RuleSetSnapshot stored = store.get("ACTIVE");

      assertThat(stored).isNotNull();
      assertThat(stored.version()).isEqualTo(1);
      assertThat(stored.snapshotId()).isEqualTo("global-store");
    }
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
      var alert = alerts.readKeyValue();
      var notification = notifications.readKeyValue();
      assertThat(alert.key).isEqualTo(alert.value.alertId());
      assertThat(notification.key).isEqualTo(notification.value.notificationRequestId());
      assertThat(notification.value.alertId()).isEqualTo(alert.value.alertId());
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

      rulesets.pipeInput(
          "ACTIVE",
          testRuleset(
              "multi",
              1,
              List.of(
                  testRule("high", 1, 0, "HIGH", amountCondition(10_000, "BRL")),
                  testRule("critical", 1, 1, "CRITICAL", amountCondition(10_000, "BRL")))));

      transactions.pipeInput(
          "multi-tx",
          testEvent(
              "multi-event",
              "multi-tx",
              "customer-multi",
              10_000,
              Instant.parse("2026-01-01T00:02:00Z")));

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

      rulesets.pipeInput(
          "ACTIVE",
          testRuleset(
              "snapshot-2",
              2,
              List.of(testRule("amount", 2, 0, "HIGH", amountCondition(50, "BRL")))));

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

      rulesets.pipeInput(
          "ACTIVE",
          testRuleset(
              "count-snapshot",
              1,
              List.of(testRule("count", 1, 0, "HIGH", countCondition(600, 2)))));

      transactions.pipeInput(
          "count-1",
          testEvent(
              "count-event-1",
              "count-1",
              "customer-count",
              1,
              Instant.parse("2026-01-01T00:01:00Z")));

      transactions.pipeInput(
          "count-2",
          testEvent(
              "count-event-2",
              "count-2",
              "customer-count",
              1,
              Instant.parse("2026-01-01T00:02:00Z")));

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

      rulesets.pipeInput(
          "ACTIVE",
          testRuleset(
              "late-count", 1, List.of(testRule("count", 1, 0, "HIGH", countCondition(600, 3)))));

      transactions.pipeInput(
          "late-2",
          testEvent(
              "late-event-2", "late-2", "customer-late", 1, Instant.parse("2026-01-01T00:02:00Z")));

      transactions.pipeInput(
          "late-1",
          testEvent(
              "late-event-1", "late-1", "customer-late", 1, Instant.parse("2026-01-01T00:01:00Z")));

      transactions.pipeInput(
          "late-3",
          testEvent(
              "late-event-3", "late-3", "customer-late", 1, Instant.parse("2026-01-01T00:03:00Z")));

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

      rulesets.pipeInput(
          "ACTIVE",
          testRuleset(
              "insufficient", 1, List.of(testRule("count", 1, 0, "HIGH", countCondition(600, 2)))));

      transactions.pipeInput(
          "new",
          testEvent(
              "new-event",
              "new",
              "customer-insufficient",
              1,
              Instant.parse("2026-01-01T01:00:00Z")));

      transactions.pipeInput(
          "old",
          testEvent(
              "old-event",
              "old",
              "customer-insufficient",
              1,
              Instant.parse("2026-01-01T00:00:00Z")));

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

  @Test
  void routesMalformedJsonWithoutStoppingTheNextValidEvent() {
    Properties properties = new Properties();
    properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "malformed-json-test");
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
              Serdes.ByteArray().serializer());
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
      transactions.pipeInput("malformed", "not-json".getBytes(StandardCharsets.UTF_8));
      transactions.pipeInput(
          "tx-1",
          new JsonSerde<>(TransactionEvent.class)
              .serializer()
              .serialize(DetectionTopology.TRANSACTION_TOPIC, event()));

      assertThat(invalid.readValue().reasonCode()).isEqualTo("INVALID_TRANSACTION_EVENT");
      assertThat(assessments.readValue().transactionId()).isEqualTo("tx-1");
    }
  }

  private static RuleSetSnapshot snapshot() {
    return testRuleset(
        "snapshot-1", 1, List.of(testRule("amount", 1, 0, "HIGH", amountCondition(10_000, "BRL"))));
  }

  @Test
  void evaluatesAllCompositeUsingTheDomainRuleEvaluator() {
    Properties properties = new Properties();
    properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "all-composite-test");
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

      RuleSetSnapshot ruleset =
          testRuleset(
              "all-ruleset",
              1,
              List.of(
                  testRule(
                      "large-and-frequent",
                      1,
                      0,
                      "HIGH",
                      allCondition(amountCondition(10_000, "BRL"), countCondition(600, 2)))));

      rulesets.pipeInput("ACTIVE", ruleset);

      transactions.pipeInput(
          "tx-all-1",
          testEvent(
              "event-all-1",
              "tx-all-1",
              "customer-all",
              100,
              Instant.parse("2026-01-01T12:00:00Z")));

      TransactionAssessment first = assessments.readValue();

      assertThat(first.status()).isEqualTo("NOT_SUSPICIOUS");

      transactions.pipeInput(
          "tx-all-2",
          testEvent(
              "event-all-2",
              "tx-all-2",
              "customer-all",
              10_000,
              Instant.parse("2026-01-01T12:05:00Z")));

      TransactionAssessment second = assessments.readValue();

      assertThat(second.status()).isEqualTo("SUSPICIOUS");

      assertThat(second.ruleResults()).hasSize(1);

      JsonNode result = second.ruleResults().getFirst();

      assertThat(result.path("ruleId").asText()).isEqualTo("large-and-frequent");

      assertThat(result.path("status").asText()).isEqualTo("MATCHED");

      assertThat(result.path("evidenceCodes").toString()).contains("ALL_MATCHED");
    }
  }

  @Test
  void evaluatesEachCountWindowUsingItsDeclaredWindowSize() {
    Properties properties = new Properties();
    properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "different-windows-test");
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

      RuleSetSnapshot ruleset =
          testRuleset(
              "different-windows",
              1,
              List.of(
                  testRule("count-60", 1, 0, "MEDIUM", countCondition(60, 2)),
                  testRule("count-600", 1, 1, "HIGH", countCondition(600, 2))));

      rulesets.pipeInput("ACTIVE", ruleset);

      transactions.pipeInput(
          "tx-window-1",
          testEvent(
              "event-window-1",
              "tx-window-1",
              "customer-window",
              100,
              Instant.parse("2026-01-01T12:00:00Z")));

      assessments.readValue();

      transactions.pipeInput(
          "tx-window-2",
          testEvent(
              "event-window-2",
              "tx-window-2",
              "customer-window",
              100,
              Instant.parse("2026-01-01T12:05:00Z")));

      TransactionAssessment assessment = assessments.readValue();

      JsonNode sixtySecondResult =
          assessment.ruleResults().stream()
              .filter(result -> "count-60".equals(result.path("ruleId").asText()))
              .findFirst()
              .orElseThrow();

      JsonNode tenMinuteResult =
          assessment.ruleResults().stream()
              .filter(result -> "count-600".equals(result.path("ruleId").asText()))
              .findFirst()
              .orElseThrow();

      assertThat(sixtySecondResult.path("status").asText()).isEqualTo("NO_MATCH");

      assertThat(tenMinuteResult.path("status").asText()).isEqualTo("MATCHED");

      assertThat(assessment.status()).isEqualTo("SUSPICIOUS");
    }
  }

  @Test
  void marksStatefulRuleNotEvaluatedWhenEventFallsOutsideRetentionBasedOnStreamTime() {
    Properties properties = new Properties();
    properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "stream-time-retention-test");
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

      RuleSetSnapshot ruleset =
          testRuleset(
              "stream-time", 1, List.of(testRule("count", 1, 0, "HIGH", countCondition(600, 2))));

      rulesets.pipeInput("ACTIVE", ruleset);

      /*
       * Advances stream time to 01:00 using customer A.
       */
      transactions.pipeInput(
          "tx-new",
          testEvent(
              "event-new", "tx-new", "customer-a", 100, Instant.parse("2026-01-01T01:00:00Z")));

      TransactionAssessment recentAssessment = assessments.readValue();

      assertThat(recentAssessment.status()).isEqualTo("NOT_SUSPICIOUS");

      /*
       * Different customer, but occurredAt is one hour behind
       * the current stream time.
       *
       * Retention = 15 minutes, therefore the engine cannot
       * guarantee complete history for this event.
       */
      transactions.pipeInput(
          "tx-old",
          testEvent(
              "event-old", "tx-old", "customer-b", 100, Instant.parse("2026-01-01T00:00:00Z")));

      TransactionAssessment oldAssessment = assessments.readValue();

      assertThat(oldAssessment.status()).isEqualTo("INCONCLUSIVE");

      assertThat(oldAssessment.ruleResults()).hasSize(1);

      JsonNode result = oldAssessment.ruleResults().getFirst();

      assertThat(result.path("status").asText()).isEqualTo("NOT_EVALUATED");

      assertThat(result.path("evidenceCodes").toString()).contains("HISTORY_UNAVAILABLE");
    }
  }

  @Test
  void quarantinesSameEventIdWhenItsFingerprintChanges() {
    Properties properties = new Properties();
    properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "event-fingerprint-conflict-test");
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

      rulesets.pipeInput(
          "ACTIVE",
          testRuleset(
              "fingerprint-ruleset",
              1,
              List.of(testRule("amount", 1, 0, "HIGH", amountCondition(10_000, "BRL")))));

      TransactionEvent original =
          testEvent(
              "event-fingerprint",
              "tx-fingerprint",
              "customer-fingerprint",
              100,
              Instant.parse("2026-01-01T12:00:00Z"));

      TransactionEvent conflictingReplay =
          testEvent(
              "event-fingerprint",
              "tx-fingerprint",
              "customer-fingerprint",
              20_000,
              Instant.parse("2026-01-01T12:00:00Z"));

      transactions.pipeInput(original.transactionId(), original);

      TransactionAssessment firstAssessment = assessments.readValue();

      assertThat(firstAssessment.status()).isEqualTo("NOT_SUSPICIOUS");

      transactions.pipeInput(conflictingReplay.transactionId(), conflictingReplay);

      assertThat(quarantined.isEmpty())
          .as("conflicting replay must be routed to quarantine")
          .isFalse();

      QuarantinedEventReference conflict = quarantined.readValue();

      assertThat(conflict.eventId()).isEqualTo("event-fingerprint");

      assertThat(conflict.transactionId()).isEqualTo("tx-fingerprint");

      assertThat(conflict.reasonCode()).isEqualTo("EVENT_IDENTITY_CONFLICT");

      assertThat(assessments.isEmpty()).isTrue();
    }
  }

  @Test
  void keepsLastValidRulesetWhenNewerSnapshotContainsUnsupportedDsl() {
    Properties properties = new Properties();
    properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "invalid-dsl-ruleset-test");
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

      RuleSetSnapshot valid =
          testRuleset(
              "valid-before-invalid",
              1,
              List.of(testRule("amount", 1, 0, "HIGH", amountCondition(10_000, "BRL"))));

      JsonNode unsupportedCondition =
          JsonNodeFactory.instance
              .objectNode()
              .put("type", "SUM_WINDOW")
              .put("windowSeconds", 600)
              .put("minimumAmountMinor", 50_000);

      RuleSetSnapshot invalid =
          testRuleset(
              "unsupported-dsl",
              2,
              List.of(testRule("unsupported", 1, 0, "CRITICAL", unsupportedCondition)));

      rulesets.pipeInput("ACTIVE", valid);
      rulesets.pipeInput("ACTIVE", invalid);

      transactions.pipeInput(
          "tx-after-invalid",
          testEvent(
              "event-after-invalid",
              "tx-after-invalid",
              "customer-after-invalid",
              10_000,
              Instant.parse("2026-01-01T12:00:00Z")));

      TransactionAssessment assessment = assessments.readValue();

      assertThat(assessment.rulesetVersion()).isEqualTo(1);

      assertThat(assessment.status()).isEqualTo("SUSPICIOUS");

      var store =
          driver.<String, RuleSetSnapshot>getKeyValueStore(
              DetectionTopology.ACTIVE_RULESETS_GLOBAL_STORE);

      assertThat(store.get("ACTIVE").version()).isEqualTo(1);

      assertThat(store.get("ACTIVE").snapshotId()).isEqualTo("valid-before-invalid");
    }
  }

  @Test
  void allowsReprocessingAfterTheTwentyFourHourIdentityHorizonExpires() {
    Properties properties = new Properties();
    properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "identity-retention-test");
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

      rulesets.pipeInput(
          "ACTIVE",
          testRuleset(
              "identity-retention",
              1,
              List.of(testRule("amount", 1, 0, "HIGH", amountCondition(10_000, "BRL")))));

      TransactionEvent original =
          testEvent(
              "event-retention",
              "tx-retention",
              "customer-retention",
              100,
              Instant.parse("2026-01-01T00:00:00Z"));

      transactions.pipeInput(original.transactionId(), original);

      TransactionAssessment first = assessments.readValue();

      /*
       * Advance stream time by 25 hours using another event.
       */
      transactions.pipeInput(
          "tx-stream-advance",
          testEvent(
              "event-stream-advance",
              "tx-stream-advance",
              "customer-stream-advance",
              100,
              Instant.parse("2026-01-02T01:00:00Z")));

      assessments.readValue();

      /*
       * Same exact event after the 24h identity horizon.
       * It is no longer protected by the online dedup window,
       * so it may be evaluated again.
       */
      transactions.pipeInput(original.transactionId(), original);

      assertThat(assessments.isEmpty())
          .as("identity and dedup entries older than 24h must be treated as expired")
          .isFalse();

      TransactionAssessment replay = assessments.readValue();

      /*
       * Re-evaluation still represents the same logical assessment.
       */
      assertThat(replay.assessmentId()).isEqualTo(first.assessmentId());
    }
  }

  @Test
  void keepsLastValidRulesetWhenNewerSnapshotHasInvalidContentHash() {
    Properties properties = new Properties();
    properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "invalid-ruleset-hash-test");
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

      RuleSetSnapshot valid =
          testRuleset(
              "valid-hash-v1",
              1,
              List.of(testRule("amount", 1, 0, "HIGH", amountCondition(10_000, "BRL"))));

      RuleSetSnapshot validVersionTwo =
          testRuleset(
              "tampered-v2",
              2,
              List.of(testRule("amount", 2, 0, "CRITICAL", amountCondition(1, "BRL"))));

      RuleSetSnapshot tampered =
          new RuleSetSnapshot(
              validVersionTwo.schemaVersion(),
              validVersionTwo.snapshotId(),
              validVersionTwo.version(),
              "sha256:tampered",
              validVersionTwo.approvedChangeRuleVersionId(),
              validVersionTwo.rules(),
              validVersionTwo.createdAt());

      rulesets.pipeInput("ACTIVE", valid);
      rulesets.pipeInput("ACTIVE", tampered);

      transactions.pipeInput(
          "tx-after-tampered-ruleset",
          testEvent(
              "event-after-tampered-ruleset",
              "tx-after-tampered-ruleset",
              "customer-after-tampered-ruleset",
              100,
              Instant.parse("2026-01-01T12:00:00Z")));

      TransactionAssessment assessment = assessments.readValue();

      assertThat(assessment.rulesetVersion()).isEqualTo(1);

      assertThat(assessment.status()).isEqualTo("NOT_SUSPICIOUS");

      var store =
          driver.<String, RuleSetSnapshot>getKeyValueStore(
              DetectionTopology.ACTIVE_RULESETS_GLOBAL_STORE);

      assertThat(store.get("ACTIVE").version()).isEqualTo(1);
    }
  }

  @Test
  void keepsLastValidRulesetWhenNewerSnapshotViolatesContractSchema() {
    Properties properties = new Properties();
    properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "invalid-ruleset-schema-test");
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

      RuleSetSnapshot valid =
          testRuleset(
              "schema-valid-v1",
              1,
              List.of(testRule("amount", 1, 0, "HIGH", amountCondition(10_000, "BRL"))));

      /*
       * Missing mandatory evaluationOrder.
       *
       * The current compiler can understand this rule,
       * but the public contract explicitly rejects it.
       */
      ObjectNode ruleWithoutEvaluationOrder =
          JsonNodeFactory.instance
              .objectNode()
              .put("ruleId", "invalid-schema-rule")
              .put("ruleVersion", 2)
              .put("severity", "CRITICAL");

      ruleWithoutEvaluationOrder.set("definition", amountCondition(1, "BRL"));

      RuleSetSnapshot invalid =
          testRuleset("schema-invalid-v2", 2, List.of(ruleWithoutEvaluationOrder));

      rulesets.pipeInput("ACTIVE", valid);
      rulesets.pipeInput("ACTIVE", invalid);

      transactions.pipeInput(
          "tx-after-schema-invalid",
          testEvent(
              "event-after-schema-invalid",
              "tx-after-schema-invalid",
              "customer-after-schema-invalid",
              100,
              Instant.parse("2026-01-01T12:00:00Z")));

      TransactionAssessment assessment = assessments.readValue();

      assertThat(assessment.rulesetVersion()).isEqualTo(1);

      assertThat(assessment.status()).isEqualTo("NOT_SUSPICIOUS");

      var store =
          driver.<String, RuleSetSnapshot>getKeyValueStore(
              DetectionTopology.ACTIVE_RULESETS_GLOBAL_STORE);

      assertThat(store.get("ACTIVE").version()).isEqualTo(1);
    }
  }

  @Test
  void routesStructurallyInvalidEventWithoutAdvancingStreamTimeOrMutatingHistory() {
    Properties properties = new Properties();
    properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "invalid-schema-stream-time-test");
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
              Serdes.ByteArray().serializer());

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

      rulesets.pipeInput(
          "ACTIVE",
          testRuleset(
              "invalid-event-ruleset",
              1,
              List.of(testRule("count", 1, 0, "HIGH", countCondition(600, 2)))));

      String invalidPayload =
          """
          {
            "schemaVersion": 1,
            "eventId": "invalid-event",
            "transactionId": "invalid-tx",
            "customerId": "invalid-customer",
            "amountMinor": -1,
            "currency": "BRL",
            "occurredAt": "2026-01-01T01:00:00Z",
            "transactionType": "PURCHASE",
            "channel": "APP",
            "merchantCountry": "BR",
            "deviceIdHash": "device",
            "traceId": "invalid-trace"
          }
          """;

      transactions.pipeInput("invalid-tx", invalidPayload.getBytes(StandardCharsets.UTF_8));

      assertThat(invalid.isEmpty())
          .as("structurally invalid event must be routed to invalid topic")
          .isFalse();

      assertThat(invalid.readValue().reasonCode()).isEqualTo("INVALID_TRANSACTION_EVENT");

      assertThat(assessments.isEmpty())
          .as("structurally invalid event must not produce an assessment")
          .isTrue();

      TransactionEvent valid =
          testEvent(
              "valid-after-invalid",
              "valid-tx-after-invalid",
              "valid-customer",
              100,
              Instant.parse("2026-01-01T00:00:00Z"));

      byte[] serializedValid =
          new JsonSerde<>(TransactionEvent.class)
              .serializer()
              .serialize(DetectionTopology.TRANSACTION_TOPIC, valid);

      transactions.pipeInput(valid.transactionId(), serializedValid);

      TransactionAssessment assessment = assessments.readValue();

      assertThat(assessment.status())
          .as("invalid event timestamp must not advance detection stream time")
          .isEqualTo("NOT_SUSPICIOUS");
    }
  }

  @Test
  void marksOutOfOrderEventAsLateWhileStillEvaluatingIt() {
    Properties properties = new Properties();
    properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "late-flag-test");
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

      rulesets.pipeInput(
          "ACTIVE",
          testRuleset(
              "late-flag-ruleset",
              1,
              List.of(testRule("amount", 1, 0, "HIGH", amountCondition(10_000, "BRL")))));

      transactions.pipeInput(
          "late-flag-newer",
          testEvent(
              "late-flag-event-newer",
              "late-flag-newer",
              "late-flag-customer",
              100,
              Instant.parse("2026-01-01T00:02:00Z")));

      TransactionAssessment newer = assessments.readValue();

      assertThat(newer.late()).isFalse();

      transactions.pipeInput(
          "late-flag-older",
          testEvent(
              "late-flag-event-older",
              "late-flag-older",
              "late-flag-customer",
              100,
              Instant.parse("2026-01-01T00:01:00Z")));

      TransactionAssessment older = assessments.readValue();

      assertThat(older.status()).isEqualTo("NOT_SUSPICIOUS");

      assertThat(older.late()).isTrue();
    }
  }

  private static RuleSetSnapshot testRuleset(
      String snapshotId, long version, List<JsonNode> rules) {

    RuleSetSnapshot unsigned =
        new RuleSetSnapshot(
            1,
            snapshotId,
            version,
            "",
            "approved-" + snapshotId,
            rules,
            Instant.parse("2026-01-01T00:00:00Z"));

    return new RuleSetSnapshot(
        unsigned.schemaVersion(),
        unsigned.snapshotId(),
        unsigned.version(),
        rulesetContentHash(unsigned),
        unsigned.approvedChangeRuleVersionId(),
        unsigned.rules(),
        unsigned.createdAt());
  }

  private static String rulesetContentHash(RuleSetSnapshot snapshot) {

    JsonNode serialized = MAPPER.valueToTree(snapshot);

    ObjectNode unsigned = ((ObjectNode) serialized).deepCopy();

    unsigned.remove("contentHash");

    try {
      byte[] canonicalPayload = MAPPER.writeValueAsBytes(canonical(unsigned));

      byte[] digest = MessageDigest.getInstance("SHA-256").digest(canonicalPayload);

      return "sha256:" + HexFormat.of().formatHex(digest);
    } catch (NoSuchAlgorithmException exception) {
      throw new IllegalStateException("SHA_256_NOT_AVAILABLE", exception);
    } catch (Exception exception) {
      throw new IllegalStateException("RULESET_HASH_FAILED", exception);
    }
  }

  private static JsonNode canonical(JsonNode value) {

    if (value.isObject()) {
      ObjectNode ordered = MAPPER.createObjectNode();

      List<String> names = new ArrayList<>();

      value.fieldNames().forEachRemaining(names::add);

      names.stream()
          .sorted(Comparator.naturalOrder())
          .forEach(name -> ordered.set(name, canonical(value.get(name))));

      return ordered;
    }

    if (value.isArray()) {
      ArrayNode ordered = MAPPER.createArrayNode();

      value.forEach(item -> ordered.add(canonical(item)));

      return ordered;
    }

    return value;
  }

  private static JsonNode testRule(
      String ruleId, int ruleVersion, int evaluationOrder, String severity, JsonNode definition) {

    var rule = JsonNodeFactory.instance.objectNode();

    rule.put("ruleId", ruleId);
    rule.put("ruleVersion", ruleVersion);
    rule.put("evaluationOrder", evaluationOrder);
    rule.put("severity", severity);
    rule.set("definition", definition);

    return rule;
  }

  private static JsonNode amountCondition(long amountMinor, String currency) {

    return JsonNodeFactory.instance
        .objectNode()
        .put("type", "AMOUNT_THRESHOLD")
        .put("amountMinor", amountMinor)
        .put("currency", currency);
  }

  private static JsonNode countCondition(int windowSeconds, int minimumCount) {

    return JsonNodeFactory.instance
        .objectNode()
        .put("type", "COUNT_WINDOW")
        .put("windowSeconds", windowSeconds)
        .put("minimumCount", minimumCount);
  }

  private static JsonNode allCondition(JsonNode... children) {

    var definition = JsonNodeFactory.instance.objectNode().put("type", "ALL");

    var childrenNode = definition.putArray("children");

    for (JsonNode child : children) {
      childrenNode.add(child);
    }

    return definition;
  }

  private static TransactionEvent testEvent(
      String eventId,
      String transactionId,
      String customerId,
      long amountMinor,
      Instant occurredAt) {

    return new TransactionEvent(
        1,
        eventId,
        transactionId,
        customerId,
        amountMinor,
        "BRL",
        occurredAt,
        "PURCHASE",
        "APP",
        "BR",
        "device",
        "trace-" + eventId);
  }

  private static TransactionEvent event() {
    return testEvent("event-1", "tx-1", "customer-1", 100, Instant.parse("2026-01-01T00:01:00Z"));
  }
}
