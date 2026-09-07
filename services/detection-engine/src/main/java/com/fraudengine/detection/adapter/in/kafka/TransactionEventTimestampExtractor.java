package com.fraudengine.detection.adapter.in.kafka;

import com.fraudengine.contracts.TransactionEvent;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;

final class TransactionEventTimestampExtractor implements TimestampExtractor {

  private static final JsonSerde<TransactionEvent> SERDE = new JsonSerde<>(TransactionEvent.class);

  @Override
  public long extract(ConsumerRecord<Object, Object> record, long partitionTime) {

    if (!(record.value() instanceof byte[] payload)) {
      return fallback(record, partitionTime);
    }

    try {
      TransactionEvent event = SERDE.deserializer().deserialize(record.topic(), payload);

      if (event != null && event.occurredAt() != null) {
        return event.occurredAt().toEpochMilli();
      }
    } catch (IllegalArgumentException ignored) {
      // Payload validation routes malformed records later.
    }

    return fallback(record, partitionTime);
  }

  private long fallback(ConsumerRecord<Object, Object> record, long partitionTime) {

    if (partitionTime >= 0) {
      return partitionTime;
    }

    return Math.max(record.timestamp(), 0);
  }
}
