package com.fraudengine.detection.adapter.in.kafka;

import com.fraudengine.contracts.TransactionEvent;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;

final class TransactionEventTimestampExtractor implements TimestampExtractor {

  private static final TransactionEventPayloadValidator VALIDATOR =
      new TransactionEventPayloadValidator();

  @Override
  public long extract(ConsumerRecord<Object, Object> record, long partitionTime) {
    if (!(record.value() instanceof byte[] payload)) {
      return currentStreamTime(partitionTime);
    }

    TransactionEvent event = VALIDATOR.parseValid(payload);

    if (event == null) {
      return currentStreamTime(partitionTime);
    }

    return event.occurredAt().toEpochMilli();
  }

  private long currentStreamTime(long partitionTime) {
    return Math.max(partitionTime, 0);
  }
}
