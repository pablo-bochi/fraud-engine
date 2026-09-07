package com.fraudengine.notification.adapter.out.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fraudengine.contracts.NotificationResult;
import com.fraudengine.notification.application.port.NotificationResultPublisher;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public final class KafkaNotificationResultPublisher implements NotificationResultPublisher {

  private final ObjectMapper objectMapper;
  private final KafkaTemplate<String, String> kafka;
  private final String resultTopic;

  public KafkaNotificationResultPublisher(
      ObjectMapper objectMapper,
      KafkaTemplate<String, String> kafka,
      @Value("${notification.kafka.result-topic:fraud.notification.result.v1}")
          String resultTopic) {

    this.objectMapper = Objects.requireNonNull(objectMapper);
    this.kafka = Objects.requireNonNull(kafka);
    this.resultTopic = Objects.requireNonNull(resultTopic);
  }

  @Override
  public void publish(NotificationResult result) {
    try {
      String payload = objectMapper.writeValueAsString(result);

      kafka.send(resultTopic, result.notificationRequestId(), payload).get(5, TimeUnit.SECONDS);

    } catch (Exception error) {
      throw new IllegalStateException("NOTIFICATION_RESULT_PUBLISH_FAILED", error);
    }
  }
}
