package com.fraudengine.notification.adapter.in.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fraudengine.contracts.CustomerNotificationRequested;
import com.fraudengine.contracts.NotificationResult;
import com.fraudengine.notification.application.NotificationHandler;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public final class NotificationRequestedConsumer {

  private final NotificationHandler handler;
  private final ObjectMapper objectMapper;
  private final KafkaTemplate<String, String> kafka;
  private final String resultTopic;

  public NotificationRequestedConsumer(
      NotificationHandler handler,
      ObjectMapper objectMapper,
      KafkaTemplate<String, String> kafka,
      @Value("${notification.kafka.result-topic:fraud.notification.result.v1}")
          String resultTopic) {
    this.handler = Objects.requireNonNull(handler);
    this.objectMapper = Objects.requireNonNull(objectMapper);
    this.kafka = Objects.requireNonNull(kafka);
    this.resultTopic = Objects.requireNonNull(resultTopic);
  }

  @KafkaListener(
      topics = "${notification.kafka.request-topic:fraud.notification.requested.v1}")
  public void consume(ConsumerRecord<String, String> record) {
    CustomerNotificationRequested request = deserialize(record.value());

    if (!request.notificationRequestId().equals(record.key())) {
      throw new IllegalStateException("NOTIFICATION_REQUEST_KEY_MISMATCH");
    }

    NotificationResult result = handler.handle(request);

    publish(result);
  }

  private CustomerNotificationRequested deserialize(String payload) {
    try {
      return objectMapper.readValue(
          payload,
          CustomerNotificationRequested.class);
    } catch (Exception error) {
      throw new IllegalStateException(
          "INVALID_NOTIFICATION_REQUEST",
          error);
    }
  }

  private void publish(NotificationResult result) {
    try {
      String payload = objectMapper.writeValueAsString(result);

      kafka
          .send(
              resultTopic,
              result.notificationRequestId(),
              payload)
          .get(5, TimeUnit.SECONDS);
    } catch (Exception error) {
      throw new IllegalStateException(
          "NOTIFICATION_RESULT_PUBLISH_FAILED",
          error);
    }
  }
}
