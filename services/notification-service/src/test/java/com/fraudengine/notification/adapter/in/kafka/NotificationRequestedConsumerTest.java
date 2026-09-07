package com.fraudengine.notification.adapter.in.kafka;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fraudengine.contracts.CustomerNotificationRequested;
import com.fraudengine.contracts.NotificationResult;
import com.fraudengine.notification.application.NotificationHandler;
import com.fraudengine.notification.application.port.CustomerContactPort;
import com.fraudengine.notification.application.port.NotificationChannelPort;
import com.fraudengine.notification.application.port.NotificationDeliveryPort;
import com.fraudengine.notification.application.port.NotificationResultPublisher;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;

class NotificationRequestedConsumerTest {

  private static final Instant NOW = Instant.parse("2026-09-07T12:00:00Z");

  private static final ObjectMapper MAPPER =
      new ObjectMapper()
          .findAndRegisterModules()
          .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);

  @Test
  void replaysPersistedSentResultAfterPublishFailureWithoutSendingAgain() throws Exception {

    var deliveryPort = new InMemoryDeliveryPort();

    CustomerContactPort contactPort =
        customerId -> new CustomerContactPort.CustomerContact("customer@example.test");

    var channelSendCount = new AtomicInteger();

    NotificationChannelPort channelPort =
        (request, contact) -> {
          channelSendCount.incrementAndGet();

          return new NotificationChannelPort.ChannelResult("EMAIL", "provider-123");
        };

    var handler =
        new NotificationHandler(
            deliveryPort, contactPort, channelPort, Clock.fixed(NOW, ZoneOffset.UTC));

    var publisher = new FailOncePublisher();

    var consumer = new NotificationRequestedConsumer(handler, MAPPER, publisher);

    CustomerNotificationRequested request = request();

    ConsumerRecord<String, String> record =
        new ConsumerRecord<>(
            "fraud.notification.requested.v1",
            0,
            0,
            request.notificationRequestId(),
            MAPPER.writeValueAsString(request));

    assertThatThrownBy(() -> consumer.consume(record))
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("kafka unavailable");

    assertThat(deliveryPort.sentResult).isNotNull();
    assertThat(deliveryPort.sentResult.status()).isEqualTo("SENT");
    assertThat(channelSendCount).hasValue(1);
    assertThat(publisher.attempts).hasValue(1);

    consumer.consume(record);

    assertThat(channelSendCount).hasValue(1);
    assertThat(publisher.attempts).hasValue(2);

    assertThat(publisher.successfulResults).containsExactly(deliveryPort.sentResult);
  }

  @Test
  void rejectsNotificationRequestWithContactDataOutsideCanonicalContract() throws Exception {

    var deliveryPort = new InMemoryDeliveryPort();

    CustomerContactPort contactPort =
        customerId -> new CustomerContactPort.CustomerContact("customer@example.test");

    NotificationChannelPort channelPort =
        (request, contact) -> new NotificationChannelPort.ChannelResult("EMAIL", "provider-123");

    var handler =
        new NotificationHandler(
            deliveryPort, contactPort, channelPort, Clock.fixed(NOW, ZoneOffset.UTC));

    NotificationResultPublisher publisher = result -> {};

    var consumer = new NotificationRequestedConsumer(handler, MAPPER, publisher);

    String payload =
        """
        {
          "schemaVersion": 1,
          "notificationRequestId": "notification-123",
          "alertId": "alert-123",
          "customerId": "customer-123",
          "transactionId": "transaction-123",
          "category": "SUSPICIOUS_TRANSACTION",
          "templateId": "suspicious-transaction-v1",
          "locale": "pt-BR",
          "requestedAt": "2026-09-07T11:59:00Z",
          "traceId": "trace-123",
          "email": "customer@example.test"
        }
        """;

    ConsumerRecord<String, String> record =
        new ConsumerRecord<>("fraud.notification.requested.v1", 0, 0, "notification-123", payload);

    assertThatThrownBy(() -> consumer.consume(record))
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("INVALID_NOTIFICATION_REQUEST");

    assertThat(deliveryPort.sentResult).isNull();
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
        Instant.parse("2026-09-07T11:59:00Z"),
        "trace-123");
  }

  private static final class InMemoryDeliveryPort implements NotificationDeliveryPort {

    private NotificationResult sentResult;

    @Override
    public ClaimResult claim(CustomerNotificationRequested request, Instant claimedAt) {

      if (sentResult != null) {
        return new ClaimResult.AlreadySent(sentResult);
      }

      return new ClaimResult.Acquired(1);
    }

    @Override
    public NotificationResult markSent(
        CustomerNotificationRequested request,
        String channel,
        String providerReference,
        int attemptCount,
        Instant sentAt) {

      sentResult =
          new NotificationResult(
              request.schemaVersion(),
              request.notificationRequestId(),
              channel,
              "SENT",
              attemptCount,
              providerReference,
              null,
              sentAt,
              request.traceId());

      return sentResult;
    }

    @Override
    public NotificationResult markFailed(
        CustomerNotificationRequested request,
        String channel,
        int attemptCount,
        String reasonCode,
        Instant failedAt) {

      throw new AssertionError("markFailed must not be called");
    }
  }

  private static final class FailOncePublisher implements NotificationResultPublisher {

    private final AtomicInteger attempts = new AtomicInteger();

    private final List<NotificationResult> successfulResults = new ArrayList<>();

    @Override
    public void publish(NotificationResult result) {
      if (attempts.incrementAndGet() == 1) {
        throw new IllegalStateException("kafka unavailable");
      }

      successfulResults.add(result);
    }
  }
}
