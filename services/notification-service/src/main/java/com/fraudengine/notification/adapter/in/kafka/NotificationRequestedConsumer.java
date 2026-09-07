package com.fraudengine.notification.adapter.in.kafka;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fraudengine.contracts.CustomerNotificationRequested;
import com.fraudengine.contracts.NotificationResult;
import com.fraudengine.contracts.schema.SchemaValidator;
import com.fraudengine.notification.application.NotificationHandler;
import com.fraudengine.notification.application.port.NotificationResultPublisher;
import java.util.Objects;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public final class NotificationRequestedConsumer {

  private static final String REQUEST_CONTRACT = "customer-notification-requested-v1";

  private final NotificationHandler handler;
  private final ObjectMapper objectMapper;
  private final NotificationResultPublisher resultPublisher;
  private final SchemaValidator schemaValidator = SchemaValidator.draft7();

  public NotificationRequestedConsumer(
      NotificationHandler handler,
      ObjectMapper objectMapper,
      NotificationResultPublisher resultPublisher) {

    this.handler = Objects.requireNonNull(handler);
    this.objectMapper = Objects.requireNonNull(objectMapper);
    this.resultPublisher = Objects.requireNonNull(resultPublisher);
  }

  @KafkaListener(topics = "${notification.kafka.request-topic:fraud.notification.requested.v1}")
  public void consume(ConsumerRecord<String, String> record) {

    CustomerNotificationRequested request = deserialize(record.value());

    if (!request.notificationRequestId().equals(record.key())) {
      throw new IllegalStateException("NOTIFICATION_REQUEST_KEY_MISMATCH");
    }

    NotificationResult result = handler.handle(request);

    resultPublisher.publish(result);
  }

  private CustomerNotificationRequested deserialize(String payload) {

    try {
      JsonNode document = objectMapper.readTree(payload);

      var violations = schemaValidator.validate(REQUEST_CONTRACT, document);

      if (!violations.isEmpty()) {
        throw new IllegalArgumentException("Notification request violates canonical schema");
      }

      return objectMapper.treeToValue(document, CustomerNotificationRequested.class);

    } catch (Exception error) {
      throw new IllegalStateException("INVALID_NOTIFICATION_REQUEST", error);
    }
  }
}
