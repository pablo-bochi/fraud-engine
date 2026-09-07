package com.fraudengine.detection.config;

import com.fraudengine.detection.adapter.in.kafka.DetectionTopology;
import com.fraudengine.detection.health.StreamsReadinessHealthIndicator;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration(proxyBeanMethods = false)
public class KafkaStreamsConfiguration {
  public Properties properties(String bootstrapServers) {
    Properties properties = new Properties();
    properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "fraud-detection-engine");
    properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
    properties.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
    properties.put(
        StreamsConfig.consumerPrefix(ConsumerConfig.ISOLATION_LEVEL_CONFIG), "read_committed");
    properties.put(
        StreamsConfig.consumerPrefix(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG), "earliest");
    return properties;
  }

  @Bean
  StreamsReadinessHealthIndicator streamsReadinessHealthIndicator() {
    return new StreamsReadinessHealthIndicator();
  }

  @Bean
  DetectionTopology detectionTopology(StreamsReadinessHealthIndicator readiness) {
    return new DetectionTopology(readiness);
  }

  @Bean(initMethod = "start", destroyMethod = "close")
  KafkaStreams kafkaStreams(
      DetectionTopology topology,
      @Value("${spring.kafka.bootstrap-servers:localhost:9094}") String bootstrapServers,
      @Value("${detection.kafka.application-id:fraud-detection-engine}") String applicationId) {
    Properties properties = properties(bootstrapServers);
    properties.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
    return new KafkaStreams(topology.build(), properties);
  }
}
