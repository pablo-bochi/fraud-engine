package com.fraudengine.notification.adapter.in.kafka;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fraudengine.contracts.CustomerNotificationRequested;
import com.fraudengine.contracts.NotificationResult;
import com.fraudengine.notification.NotificationApplication;
import com.fraudengine.notification.application.port.CustomerContactPort;
import com.fraudengine.notification.application.port.NotificationChannelPort;
import com.fraudengine.notification.application.port.NotificationDeliveryPort;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
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
import org.springframework.context.annotation.Configuration;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import org.springframework.context.annotation.Profile;
import org.springframework.context.annotation.Primary;

@Testcontainers
class NotificationConsumerIntegrationTest {

  private static final String REQUEST_TOPIC = "fraud.notification.requested.v1";
  private static final String RESULT_TOPIC = "fraud.notification.result.v1";

  private static final Instant NOW = Instant.parse("2026-09-06T18:00:00Z");

  private static final ObjectMapper MAPPER =
      new ObjectMapper().findAndRegisterModules();

  @Container
  static final KafkaContainer KAFKA =
      new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.8.0"));

  @Test
  void consumesNotificationRequestAndPublishesSentResult() throws Exception {
    createTopics();

    String groupId = "notification-it-" + UUID.randomUUID();

    try (ConfigurableApplicationContext application =
        new SpringApplicationBuilder(
                NotificationApplication.class,
                NotificationTestConfiguration.class)
            .run(
                "--spring.profiles.active=notification-consumer-it",
                "--notification.persistence.enabled=false",
                "--notification.contact.fixture.enabled=false",
                "--notification.mail.enabled=false",
                "--spring.autoconfigure.exclude="
                    + "org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration,"
                    + "org.springframework.boot.autoconfigure.flyway.FlywayAutoConfiguration",
                "--spring.kafka.bootstrap-servers=" + KAFKA.getBootstrapServers(),
                "--spring.kafka.consumer.group-id=" + groupId,
                "--spring.kafka.consumer.auto-offset-reset=earliest",
                "--spring.kafka.consumer.enable-auto-commit=false",
                "--spring.kafka.consumer.key-deserializer="
                    + StringDeserializer.class.getName(),
                "--spring.kafka.consumer.value-deserializer="
                    + StringDeserializer.class.getName(),
                "--spring.kafka.producer.key-serializer="
                    + StringSerializer.class.getName(),
                "--spring.kafka.producer.value-serializer="
                    + StringSerializer.class.getName())) {

      publishRequest(request());

      NotificationResult result = readResult();

      assertThat(result.notificationRequestId()).isEqualTo("notification-123");
      assertThat(result.channel()).isEqualTo("EMAIL");
      assertThat(result.status()).isEqualTo("SENT");
      assertThat(result.attemptCount()).isEqualTo(1);
      assertThat(result.providerReference()).isEqualTo("mailpit-message-123");
      assertThat(result.reasonCode()).isNull();
      assertThat(result.updatedAt()).isEqualTo(NOW);
      assertThat(result.traceId()).isEqualTo("trace-123");
    }
  }

  private static void createTopics() throws Exception {
    Properties properties = new Properties();
    properties.put(
        ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
        KAFKA.getBootstrapServers());

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

  private static void publishRequest(CustomerNotificationRequested request)
      throws Exception {

    Properties properties = new Properties();
    properties.put(
        ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
        KAFKA.getBootstrapServers());

    try (KafkaProducer<String, String> producer =
        new KafkaProducer<>(
            properties,
            new StringSerializer(),
            new StringSerializer())) {

      producer
          .send(
              new ProducerRecord<>(
                  REQUEST_TOPIC,
                  request.notificationRequestId(),
                  MAPPER.writeValueAsString(request)))
          .get(10, TimeUnit.SECONDS);
    }
  }

  private static NotificationResult readResult() throws Exception {
    Properties properties = new Properties();

    properties.put(
        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
        KAFKA.getBootstrapServers());
    properties.put(
        ConsumerConfig.GROUP_ID_CONFIG,
        "result-reader-" + UUID.randomUUID());
    properties.put(
        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
        "earliest");

    try (KafkaConsumer<String, String> consumer =
        new KafkaConsumer<>(
            properties,
            new StringDeserializer(),
            new StringDeserializer())) {

      consumer.subscribe(List.of(RESULT_TOPIC));

      long deadline =
          System.nanoTime() + Duration.ofSeconds(10).toNanos();

      while (System.nanoTime() < deadline) {
        for (var record : consumer.poll(Duration.ofMillis(250))) {
          assertThat(record.key()).isEqualTo("notification-123");

          return MAPPER.readValue(
              record.value(),
              NotificationResult.class);
        }
      }
    }

    throw new AssertionError("NOTIFICATION_RESULT_NOT_OBSERVED");
  }

  private static CustomerNotificationRequested request() {
    return new CustomerNotificationRequested(
        1,
        "notification-123",
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
  @Profile("notification-consumer-it")
  static class NotificationTestConfiguration {

    @Bean
    NotificationDeliveryPort notificationDeliveryPort() {
      return new NotificationDeliveryPort() {

        @Override
        public ClaimResult claim(
            CustomerNotificationRequested request,
            Instant claimedAt) {
          return new ClaimResult.Acquired(1);
        }

        @Override
        public NotificationResult markSent(
            CustomerNotificationRequested request,
            String channel,
            String providerReference,
            int attemptCount,
            Instant sentAt) {

          return new NotificationResult(
              request.schemaVersion(),
              request.notificationRequestId(),
              channel,
              "SENT",
              attemptCount,
              providerReference,
              null,
              sentAt,
              request.traceId());
        }

        @Override
        public NotificationResult markFailed(
            CustomerNotificationRequested request,
            String channel,
            int attemptCount,
            String reasonCode,
            Instant failedAt) {

          throw new AssertionError(
              "markFailed must not be called in the Kafka happy path");
        }
      };
    }

    @Bean
    CustomerContactPort customerContactPort() {
      return customerId ->
          new CustomerContactPort.CustomerContact(
              "customer-123@example.test");
    }

    @Bean
    NotificationChannelPort notificationChannelPort() {
      return (request, contact) ->
          new NotificationChannelPort.ChannelResult(
              "EMAIL",
              "mailpit-message-123");
    }

    @Bean
    @Primary
    Clock fixedNotificationClock() {
      return Clock.fixed(NOW, ZoneOffset.UTC);
    }

  }
}
