package com.fraudengine.detection.adapter.in.kafka;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fraudengine.contracts.RuleSetSnapshot;
import com.fraudengine.contracts.TransactionAssessment;
import com.fraudengine.contracts.TransactionEvent;
import com.fraudengine.detection.DetectionEngineApplication;
import com.fraudengine.detection.health.StreamsReadinessHealthIndicator;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HexFormat;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.junit.jupiter.api.Test;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

@Testcontainers
class DetectionKafkaIntegrationTest {
  private static final ObjectMapper MAPPER = new ObjectMapper().findAndRegisterModules();

  @Container
  static final KafkaContainer KAFKA =
      new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.8.0"));

  @Test
  void startsTheApplicationAndPublishesACommittedAssessmentWithoutPostgres() throws Exception {
    createTopics();
    try (ConfigurableApplicationContext application =
        new SpringApplicationBuilder(DetectionEngineApplication.class)
            .run(
                "--spring.kafka.bootstrap-servers=" + KAFKA.getBootstrapServers(),
                "--detection.kafka.application-id=u4-" + UUID.randomUUID(),
                "--server.port=0")) {
      StreamsReadinessHealthIndicator readiness =
          application.getBean(StreamsReadinessHealthIndicator.class);
      publishMalformedTransaction();
      publishRuleset();
      awaitRuleset(readiness);
      publishTransaction();
      assertThat(readAssessment()).isEqualTo("NOT_SUSPICIOUS");
    }
  }

  private static void createTopics() throws Exception {
    Properties properties = new Properties();
    properties.put("bootstrap.servers", KAFKA.getBootstrapServers());
    try (AdminClient admin = AdminClient.create(properties)) {
      admin
          .createTopics(
              List.of(
                  new NewTopic(DetectionTopology.RULESET_TOPIC, 1, (short) 1),
                  new NewTopic(DetectionTopology.TRANSACTION_TOPIC, 1, (short) 1),
                  new NewTopic(DetectionTopology.ASSESSMENT_TOPIC, 1, (short) 1),
                  new NewTopic(DetectionTopology.ALERT_TOPIC, 1, (short) 1),
                  new NewTopic(DetectionTopology.NOTIFICATION_TOPIC, 1, (short) 1),
                  new NewTopic(DetectionTopology.QUARANTINE_TOPIC, 1, (short) 1),
                  new NewTopic(DetectionTopology.INVALID_TOPIC, 1, (short) 1)))
          .all()
          .get();
    }
  }

  private static void awaitRuleset(StreamsReadinessHealthIndicator readiness)
      throws InterruptedException {
    long deadline = System.nanoTime() + Duration.ofSeconds(15).toNanos();
    while (System.nanoTime() < deadline) {
      if ("UP".equals(readiness.health().getStatus().getCode())) return;
      Thread.sleep(50);
    }
    throw new AssertionError("RULESET_NOT_LOADED");
  }

  private static void publishRuleset() throws Exception {
    try (KafkaProducer<String, RuleSetSnapshot> producer =
        producer(new JsonSerde<>(RuleSetSnapshot.class))) {

      ObjectNode rule = JsonNodeFactory.instance.objectNode();

      rule.put("ruleId", "limit");
      rule.put("ruleVersion", 1);
      rule.put("evaluationOrder", 0);
      rule.put("severity", "HIGH");

      rule.set(
          "definition",
          JsonNodeFactory.instance
              .objectNode()
              .put("type", "AMOUNT_THRESHOLD")
              .put("amountMinor", 100)
              .put("currency", "BRL"));

      RuleSetSnapshot ruleset = validRuleset("integration", 1, List.of(rule));

      producer.send(new ProducerRecord<>(DetectionTopology.RULESET_TOPIC, "ACTIVE", ruleset)).get();
    }
  }

  private static void publishTransaction() throws Exception {
    try (KafkaProducer<String, TransactionEvent> producer =
        producer(new JsonSerde<>(TransactionEvent.class))) {
      producer
          .send(
              new ProducerRecord<>(
                  DetectionTopology.TRANSACTION_TOPIC,
                  "integration-tx",
                  new TransactionEvent(
                      1,
                      "integration-event",
                      "integration-tx",
                      "integration-customer",
                      1,
                      "BRL",
                      Instant.now(),
                      "PURCHASE",
                      "APP",
                      "BR",
                      "device",
                      "trace")))
          .get();
    }
  }

  private static void publishMalformedTransaction() throws Exception {
    Properties properties = new Properties();
    properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
    try (KafkaProducer<String, byte[]> producer =
        new KafkaProducer<>(
            properties, Serdes.String().serializer(), Serdes.ByteArray().serializer())) {
      producer
          .send(
              new ProducerRecord<>(
                  DetectionTopology.TRANSACTION_TOPIC, "malformed", "not-json".getBytes()))
          .get();
    }
  }

  private static String readAssessment() {
    Properties properties = new Properties();
    properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
    properties.put(ConsumerConfig.GROUP_ID_CONFIG, "reader-" + UUID.randomUUID());
    properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    properties.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
    try (KafkaConsumer<String, TransactionAssessment> consumer =
        new KafkaConsumer<>(
            properties,
            Serdes.String().deserializer(),
            new JsonSerde<>(TransactionAssessment.class).deserializer())) {
      consumer.subscribe(List.of(DetectionTopology.ASSESSMENT_TOPIC));
      long deadline = System.nanoTime() + Duration.ofSeconds(20).toNanos();
      while (System.nanoTime() < deadline)
        for (var record : consumer.poll(Duration.ofMillis(500))) return record.value().status();
    }
    throw new AssertionError("ASSESSMENT_NOT_OBSERVED_READ_COMMITTED");
  }

  private static <T> KafkaProducer<String, T> producer(JsonSerde<T> serde) {
    Properties properties = new Properties();
    properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
    return new KafkaProducer<>(properties, Serdes.String().serializer(), serde.serializer());
  }

  private static RuleSetSnapshot validRuleset(
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
      byte[] canonicalPayload =
          MAPPER.writeValueAsString(canonical(unsigned)).getBytes(StandardCharsets.UTF_8);

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
}
