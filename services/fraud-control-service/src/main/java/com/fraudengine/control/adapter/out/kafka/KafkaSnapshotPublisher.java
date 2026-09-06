package com.fraudengine.control.adapter.out.kafka;

import com.fraudengine.control.adapter.out.persistence.SnapshotPublisher;
import java.util.concurrent.TimeUnit;
import org.springframework.kafka.core.KafkaTemplate;

public final class KafkaSnapshotPublisher implements SnapshotPublisher {
  private final KafkaTemplate<String, String> kafka;
  private final String topic;

  public KafkaSnapshotPublisher(KafkaTemplate<String, String> kafka, String topic) {
    this.kafka = kafka;
    this.topic = topic;
  }

  @Override
  public void publish(String key, String canonicalPayload) {
    try {
      kafka.send(topic, key, canonicalPayload).get(5, TimeUnit.SECONDS);
    } catch (Exception error) {
      throw new IllegalStateException("KAFKA_PUBLISH_FAILED", error);
    }
  }
}
