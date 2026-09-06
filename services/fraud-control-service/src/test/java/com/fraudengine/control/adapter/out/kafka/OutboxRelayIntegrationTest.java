package com.fraudengine.control.adapter.out.kafka;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;

@EnabledIfEnvironmentVariable(named = "KAFKA_BOOTSTRAP_SERVERS", matches = ".+")
class OutboxRelayIntegrationTest {
  private static final String TOPIC = "fraud.ruleset.active.v1";

  @Test
  void publishesTheCanonicalSnapshotWithTheActiveKeyAfterBrokerAcknowledgement() {
    KafkaSnapshotPublisher publisher =
        new KafkaSnapshotPublisher(new KafkaTemplate<>(producerFactory()), TOPIC);

    publisher.publish("ACTIVE", "{\"version\":1,\"contentHash\":\"sha256:test\"}");

    try (KafkaConsumer<String, String> consumer = consumer()) {
      consumer.subscribe(List.of(TOPIC));
      var records = consumer.poll(Duration.ofSeconds(10)).records(TOPIC);
      assertThat(records)
          .anySatisfy(
              record -> {
                assertThat(record.key()).isEqualTo("ACTIVE");
                assertThat(record.value()).contains("sha256:test");
              });
    }
  }

  private DefaultKafkaProducerFactory<String, String> producerFactory() {
    return new DefaultKafkaProducerFactory<>(
        Map.of(
            ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
            bootstrapServers(),
            ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
            StringSerializer.class,
            ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
            StringSerializer.class,
            ProducerConfig.ACKS_CONFIG,
            "all"));
  }

  private KafkaConsumer<String, String> consumer() {
    return new KafkaConsumer<>(
        Map.of(
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
            bootstrapServers(),
            ConsumerConfig.GROUP_ID_CONFIG,
            "publisher-it-" + UUID.randomUUID(),
            ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
            "earliest",
            ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
            StringDeserializer.class,
            ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
            StringDeserializer.class));
  }

  private String bootstrapServers() {
    return System.getenv("KAFKA_BOOTSTRAP_SERVERS");
  }
}
