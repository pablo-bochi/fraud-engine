package com.fraudengine.notification.adapter.in.kafka;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fraudengine.contracts.CustomerNotificationRequested;
import com.fraudengine.contracts.NotificationResult;
import com.fraudengine.notification.NotificationApplication;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.context.annotation.Profile;
import org.springframework.jdbc.core.JdbcTemplate;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

@Testcontainers
class NotificationReplayIntegrationTest {

  private static final String REQUEST_TOPIC = "fraud.notification.requested.v1";
  private static final String RESULT_TOPIC = "fraud.notification.result.v1";

  private static final Instant NOW = Instant.parse("2026-09-06T18:00:00Z");

  private static final ObjectMapper MAPPER = new ObjectMapper().findAndRegisterModules();

  @Container
  static final KafkaContainer KAFKA =
      new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.8.0"));

  @Container
  static final PostgreSQLContainer<?> POSTGRES =
      new PostgreSQLContainer<>("postgres:17.11-alpine3.24");

  @Container
  static final GenericContainer<?> MAILPIT =
      new GenericContainer<>(DockerImageName.parse("axllent/mailpit:v1.31.0"))
          .withExposedPorts(1025, 8025);

  @Test
  void replaysSentResultWithoutSendingNotificationTwice() throws Exception {
    createTopics();

    try (ConfigurableApplicationContext application =
        new SpringApplicationBuilder(NotificationApplication.class, ReplayTestConfiguration.class)
            .run(
                "--spring.profiles.active=notification-replay-it",
                "--spring.datasource.url=" + POSTGRES.getJdbcUrl(),
                "--spring.datasource.username=" + POSTGRES.getUsername(),
                "--spring.datasource.password=" + POSTGRES.getPassword(),
                "--spring.flyway.schemas=notification",
                "--spring.flyway.default-schema=notification",
                "--spring.kafka.bootstrap-servers=" + KAFKA.getBootstrapServers(),
                "--spring.kafka.consumer.group-id=notification-replay-" + UUID.randomUUID(),
                "--spring.kafka.consumer.auto-offset-reset=earliest",
                "--spring.kafka.consumer.enable-auto-commit=false",
                "--spring.kafka.consumer.key-deserializer=" + StringDeserializer.class.getName(),
                "--spring.kafka.consumer.value-deserializer=" + StringDeserializer.class.getName(),
                "--spring.kafka.producer.key-serializer=" + StringSerializer.class.getName(),
                "--spring.kafka.producer.value-serializer=" + StringSerializer.class.getName(),
                "--spring.mail.host=" + MAILPIT.getHost(),
                "--spring.mail.port=" + MAILPIT.getMappedPort(1025),
                "--notification.contact.fixture-email=customer-123@example.test",
                "--notification.mail.from-address=fraud-engine@example.test")) {

      var request = request();

      publishRequest(request);
      publishRequest(request);

      List<NotificationResult> results = readResults(2);

      assertThat(results).hasSize(2);
      assertThat(results.get(0)).isEqualTo(results.get(1));

      assertThat(results.getFirst().status()).isEqualTo("SENT");
      assertThat(results.getFirst().attemptCount()).isEqualTo(1);
      assertThat(results.getFirst().providerReference()).isNotBlank();

      JsonNode messages = awaitMailpitMessages(1);

      assertThat(messages.path("total").asInt()).isEqualTo(1);

      JsonNode message = messages.path("messages").get(0);

      assertThat(message.path("To").get(0).path("Address").asText())
          .isEqualTo("customer-123@example.test");

      assertThat(message.path("MessageID").asText())
          .isEqualTo(results.getFirst().providerReference());

      JdbcTemplate jdbc = application.getBean(JdbcTemplate.class);

      assertThat(
              jdbc.queryForObject(
                  """
                  SELECT count(*)
                  FROM notification.delivery
                  WHERE notification_request_id = ?
                  """,
                  Integer.class,
                  request.notificationRequestId()))
          .isEqualTo(1);

      assertThat(
              jdbc.queryForObject(
                  """
                  SELECT status
                  FROM notification.delivery
                  WHERE notification_request_id = ?
                  """,
                  String.class,
                  request.notificationRequestId()))
          .isEqualTo("SENT");

      assertThat(
              jdbc.queryForObject(
                  """
                  SELECT attempt_count
                  FROM notification.delivery
                  WHERE notification_request_id = ?
                  """,
                  Integer.class,
                  request.notificationRequestId()))
          .isEqualTo(1);
    }
  }

  private static void createTopics() throws Exception {
    Properties properties = new Properties();

    properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());

    try (AdminClient admin = AdminClient.create(properties)) {
      admin
          .createTopics(
              List.of(
                  new NewTopic(REQUEST_TOPIC, 1, (short) 1),
                  new NewTopic(RESULT_TOPIC, 1, (short) 1)))
          .all()
          .get(10, TimeUnit.SECONDS);
    }
  }

  private static void publishRequest(CustomerNotificationRequested request) throws Exception {

    Properties properties = new Properties();

    properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());

    try (KafkaProducer<String, String> producer =
        new KafkaProducer<>(properties, new StringSerializer(), new StringSerializer())) {

      producer
          .send(
              new ProducerRecord<>(
                  REQUEST_TOPIC,
                  request.notificationRequestId(),
                  MAPPER.writeValueAsString(request)))
          .get(10, TimeUnit.SECONDS);
    }
  }

  private static List<NotificationResult> readResults(int expected) throws Exception {

    Properties properties = new Properties();

    properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
    properties.put(ConsumerConfig.GROUP_ID_CONFIG, "result-reader-" + UUID.randomUUID());
    properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

    List<NotificationResult> results = new ArrayList<>();

    try (KafkaConsumer<String, String> consumer =
        new KafkaConsumer<>(properties, new StringDeserializer(), new StringDeserializer())) {

      consumer.subscribe(List.of(RESULT_TOPIC));

      long deadline = System.nanoTime() + Duration.ofSeconds(15).toNanos();

      while (System.nanoTime() < deadline && results.size() < expected) {
        for (var record : consumer.poll(Duration.ofMillis(250))) {
          if ("notification-replay-123".equals(record.key())) {
            results.add(MAPPER.readValue(record.value(), NotificationResult.class));
          }
        }
      }
    }

    if (results.size() != expected) {
      throw new AssertionError(
          "EXPECTED_" + expected + "_NOTIFICATION_RESULTS_BUT_OBSERVED_" + results.size());
    }

    return results;
  }

  private static CustomerNotificationRequested request() {
    return new CustomerNotificationRequested(
        1,
        "notification-replay-123",
        "alert-123",
        "customer-123",
        "transaction-123",
        "SUSPICIOUS_TRANSACTION",
        "suspicious-transaction-v1",
        "pt-BR",
        Instant.parse("2026-09-06T17:59:00Z"),
        "trace-123");
  }

  @Configuration(proxyBeanMethods = false)
  @Profile("notification-replay-it")
  static class ReplayTestConfiguration {
    @Bean
    @Primary
    Clock notificationTestClock() {
      return Clock.fixed(NOW, ZoneOffset.UTC);
    }
  }

  private static JsonNode awaitMailpitMessages(int expected) throws Exception {

    HttpClient client = HttpClient.newHttpClient();

    URI uri =
        URI.create(
            "http://" + MAILPIT.getHost() + ":" + MAILPIT.getMappedPort(8025) + "/api/v1/messages");

    long deadline = System.nanoTime() + Duration.ofSeconds(5).toNanos();

    while (System.nanoTime() < deadline) {
      HttpRequest request =
          HttpRequest.newBuilder(uri).GET().timeout(Duration.ofSeconds(2)).build();

      HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());

      assertThat(response.statusCode()).isEqualTo(200);

      JsonNode body = MAPPER.readTree(response.body());

      if (body.path("total").asInt() >= expected) {
        return body;
      }

      Thread.sleep(50);
    }

    throw new AssertionError("EXPECTED_" + expected + "_MAILPIT_MESSAGES");
  }
}
