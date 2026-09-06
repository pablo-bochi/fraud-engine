package com.fraudengine.testsupport;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.stream.StreamSupport;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;

class KafkaTransactionIT {

  @Test
  void committedTransactionIsVisibleToReadCommittedConsumer() {
    String bootstrap = System.getProperty("kafka.bootstrap", "localhost:9094");
    String marker = "u1-" + UUID.randomUUID();
    Properties producerProperties = new Properties();
    producerProperties.putAll(
        Map.of(
            ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
            bootstrap,
            ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
            StringSerializer.class,
            ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
            StringSerializer.class,
            ProducerConfig.TRANSACTIONAL_ID_CONFIG,
            marker,
            ProducerConfig.MAX_BLOCK_MS_CONFIG,
            "30000"));

    try (KafkaProducer<String, String> producer = new KafkaProducer<>(producerProperties)) {
      producer.initTransactions();
      producer.beginTransaction();
      producer.send(new ProducerRecord<>("fraud.transaction.received.v1", marker, marker));
      producer.commitTransaction();
    }

    Properties consumerProperties = new Properties();
    consumerProperties.putAll(
        Map.of(
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
            bootstrap,
            ConsumerConfig.GROUP_ID_CONFIG,
            marker,
            ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
            "earliest",
            ConsumerConfig.ISOLATION_LEVEL_CONFIG,
            "read_committed",
            ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
            StringDeserializer.class,
            ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
            StringDeserializer.class));
    try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProperties)) {
      consumer.subscribe(java.util.List.of("fraud.transaction.received.v1"));
      boolean observed =
          StreamSupport.stream(
                  consumer
                      .poll(Duration.ofSeconds(10))
                      .records("fraud.transaction.received.v1")
                      .spliterator(),
                  false)
              .anyMatch(record -> marker.equals(record.value()));
      assertThat(observed).isTrue();
    }
  }
}
