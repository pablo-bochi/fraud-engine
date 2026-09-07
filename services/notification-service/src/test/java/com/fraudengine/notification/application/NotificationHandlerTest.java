package com.fraudengine.notification.application;

import static org.assertj.core.api.Assertions.assertThat;

import com.fraudengine.contracts.CustomerNotificationRequested;
import com.fraudengine.contracts.NotificationResult;
import com.fraudengine.notification.application.port.CustomerContactPort;
import com.fraudengine.notification.application.port.NotificationChannelPort;
import com.fraudengine.notification.application.port.NotificationDeliveryPort;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class NotificationHandlerTest {

  private static final Instant NOW = Instant.parse("2026-09-06T18:00:00Z");

  @Test
  void sendsNewNotificationAndReturnsSentResult() {
    var request = request();
    var deliveryPort = new FakeNotificationDeliveryPort();

    var contactResolutionCount = new AtomicInteger();
    CustomerContactPort customerContactPort =
        customerId -> {
          contactResolutionCount.incrementAndGet();
          assertThat(customerId).isEqualTo("customer-123");
          return new CustomerContactPort.CustomerContact("customer-123@example.test");
        };

    var channelSendCount = new AtomicInteger();
    NotificationChannelPort notificationChannelPort =
        (notificationRequest, customerContact) -> {
          channelSendCount.incrementAndGet();

          assertThat(notificationRequest).isEqualTo(request);
          assertThat(customerContact.email()).isEqualTo("customer-123@example.test");

          return new NotificationChannelPort.ChannelResult("EMAIL", "mailpit-message-123");
        };

    var handler =
        new NotificationHandler(
            deliveryPort,
            customerContactPort,
            notificationChannelPort,
            Clock.fixed(NOW, ZoneOffset.UTC));

    NotificationResult result = handler.handle(request);

    assertThat(result.notificationRequestId()).isEqualTo("notification-123");
    assertThat(result.channel()).isEqualTo("EMAIL");
    assertThat(result.status()).isEqualTo("SENT");
    assertThat(result.attemptCount()).isEqualTo(1);
    assertThat(result.providerReference()).isEqualTo("mailpit-message-123");
    assertThat(result.reasonCode()).isNull();
    assertThat(result.updatedAt()).isEqualTo(NOW);
    assertThat(result.traceId()).isEqualTo("trace-123");

    assertThat(contactResolutionCount).hasValue(1);
    assertThat(channelSendCount).hasValue(1);

    assertThat(deliveryPort.claimCount).isEqualTo(1);
    assertThat(deliveryPort.markSentCount).isEqualTo(1);
  }

  @Test
  void returnsExistingSentResultWithoutSendingAgain() {
    var request = request();

    var existingResult =
        new NotificationResult(
            1,
            "notification-123",
            "EMAIL",
            "SENT",
            1,
            "mailpit-message-123",
            null,
            Instant.parse("2026-09-06T17:50:00Z"),
            "trace-123");

    NotificationDeliveryPort deliveryPort =
        new NotificationDeliveryPort() {

          @Override
          public ClaimResult claim(
              CustomerNotificationRequested notificationRequest, Instant claimedAt) {
            assertThat(notificationRequest).isEqualTo(request);
            return new ClaimResult.AlreadySent(existingResult);
          }

          @Override
          public NotificationResult markSent(
              CustomerNotificationRequested notificationRequest,
              String channel,
              String providerReference,
              int attemptCount,
              Instant sentAt) {
            throw new AssertionError("markSent must not be called for an already sent notification");
          }

          @Override
          public NotificationResult markFailed(
              CustomerNotificationRequested notificationRequest,
              String channel,
              int attemptCount,
              String reasonCode,
              Instant failedAt) {
            throw new AssertionError("markFailed must not be called for an already sent notification");
          }
        };

    CustomerContactPort customerContactPort =
        customerId -> {
          throw new AssertionError(
              "customer contact must not be resolved for an already sent notification");
        };

    NotificationChannelPort notificationChannelPort =
        (notificationRequest, customerContact) -> {
          throw new AssertionError("channel must not be called for an already sent notification");
        };

    var handler =
        new NotificationHandler(
            deliveryPort,
            customerContactPort,
            notificationChannelPort,
            Clock.fixed(NOW, ZoneOffset.UTC));

    NotificationResult result = handler.handle(request);

    assertThat(result).isEqualTo(existingResult);
  }

  @Test
  void recordsFailedDeliveryAndRetriesSameNotificationWithNextAttempt() {
    var request = request();

    var claimCount = new AtomicInteger();
    var markFailedCount = new AtomicInteger();
    var markSentCount = new AtomicInteger();

    NotificationDeliveryPort deliveryPort =
        new NotificationDeliveryPort() {

          @Override
          public ClaimResult claim(
              CustomerNotificationRequested notificationRequest, Instant claimedAt) {

            int currentClaim = claimCount.incrementAndGet();

            assertThat(notificationRequest).isEqualTo(request);
            assertThat(claimedAt).isEqualTo(NOW);

            return new ClaimResult.Acquired(currentClaim);
          }

          @Override
          public NotificationResult markSent(
              CustomerNotificationRequested notificationRequest,
              String channel,
              String providerReference,
              int attemptCount,
              Instant sentAt) {

            markSentCount.incrementAndGet();

            return new NotificationResult(
                notificationRequest.schemaVersion(),
                notificationRequest.notificationRequestId(),
                channel,
                "SENT",
                attemptCount,
                providerReference,
                null,
                sentAt,
                notificationRequest.traceId());
          }

          @Override
          public NotificationResult markFailed(
              CustomerNotificationRequested notificationRequest,
              String channel,
              int attemptCount,
              String reasonCode,
              Instant failedAt) {

            markFailedCount.incrementAndGet();

            return new NotificationResult(
                notificationRequest.schemaVersion(),
                notificationRequest.notificationRequestId(),
                channel,
                "FAILED",
                attemptCount,
                null,
                reasonCode,
                failedAt,
                notificationRequest.traceId());
          }
        };

    var contactResolutionCount = new AtomicInteger();

    CustomerContactPort customerContactPort =
        customerId -> {
          contactResolutionCount.incrementAndGet();
          return new CustomerContactPort.CustomerContact("customer-123@example.test");
        };

    var channelSendCount = new AtomicInteger();

    NotificationChannelPort notificationChannelPort =
        (notificationRequest, customerContact) -> {
          int attempt = channelSendCount.incrementAndGet();

          if (attempt == 1) {
            throw new IllegalStateException("smtp unavailable");
          }

          return new NotificationChannelPort.ChannelResult(
              "EMAIL", "mailpit-message-after-retry");
        };

    var handler =
        new NotificationHandler(
            deliveryPort,
            customerContactPort,
            notificationChannelPort,
            Clock.fixed(NOW, ZoneOffset.UTC));

    NotificationResult failedResult = handler.handle(request);

    assertThat(failedResult.status()).isEqualTo("FAILED");
    assertThat(failedResult.attemptCount()).isEqualTo(1);
    assertThat(failedResult.channel()).isEqualTo("EMAIL");
    assertThat(failedResult.providerReference()).isNull();
    assertThat(failedResult.reasonCode()).isEqualTo("CHANNEL_DELIVERY_FAILED");

    NotificationResult retriedResult = handler.handle(request);

    assertThat(retriedResult.status()).isEqualTo("SENT");
    assertThat(retriedResult.attemptCount()).isEqualTo(2);
    assertThat(retriedResult.channel()).isEqualTo("EMAIL");
    assertThat(retriedResult.providerReference()).isEqualTo("mailpit-message-after-retry");
    assertThat(retriedResult.reasonCode()).isNull();

    assertThat(claimCount).hasValue(2);
    assertThat(contactResolutionCount).hasValue(2);
    assertThat(channelSendCount).hasValue(2);
    assertThat(markFailedCount).hasValue(1);
    assertThat(markSentCount).hasValue(1);
  }

  @Test
  void recordsFailedDeliveryWhenCustomerContactCannotBeResolved() {
    var request = request();

    var markFailedCount = new AtomicInteger();

    NotificationDeliveryPort deliveryPort =
        new NotificationDeliveryPort() {

          @Override
          public ClaimResult claim(
              CustomerNotificationRequested notificationRequest, Instant claimedAt) {
            assertThat(notificationRequest).isEqualTo(request);
            assertThat(claimedAt).isEqualTo(NOW);

            return new ClaimResult.Acquired(1);
          }

          @Override
          public NotificationResult markSent(
              CustomerNotificationRequested notificationRequest,
              String channel,
              String providerReference,
              int attemptCount,
              Instant sentAt) {
            throw new AssertionError(
                "markSent must not be called when customer contact resolution fails");
          }

          @Override
          public NotificationResult markFailed(
              CustomerNotificationRequested notificationRequest,
              String channel,
              int attemptCount,
              String reasonCode,
              Instant failedAt) {

            markFailedCount.incrementAndGet();

            return new NotificationResult(
                notificationRequest.schemaVersion(),
                notificationRequest.notificationRequestId(),
                channel,
                "FAILED",
                attemptCount,
                null,
                reasonCode,
                failedAt,
                notificationRequest.traceId());
          }
        };

    CustomerContactPort customerContactPort =
        customerId -> {
          assertThat(customerId).isEqualTo("customer-123");
          throw new IllegalStateException("customer profile unavailable");
        };

    var channelSendCount = new AtomicInteger();

    NotificationChannelPort notificationChannelPort =
        (notificationRequest, customerContact) -> {
          channelSendCount.incrementAndGet();
          throw new AssertionError(
              "channel must not be called when customer contact resolution fails");
        };

    var handler =
        new NotificationHandler(
            deliveryPort,
            customerContactPort,
            notificationChannelPort,
            Clock.fixed(NOW, ZoneOffset.UTC));

    NotificationResult result = handler.handle(request);

    assertThat(result.notificationRequestId()).isEqualTo("notification-123");
    assertThat(result.status()).isEqualTo("FAILED");
    assertThat(result.channel()).isEqualTo("EMAIL");
    assertThat(result.attemptCount()).isEqualTo(1);
    assertThat(result.providerReference()).isNull();
    assertThat(result.reasonCode()).isEqualTo("CONTACT_RESOLUTION_FAILED");
    assertThat(result.updatedAt()).isEqualTo(NOW);
    assertThat(result.traceId()).isEqualTo("trace-123");

    assertThat(markFailedCount).hasValue(1);
    assertThat(channelSendCount).hasValue(0);
  }

  @Test
  void doesNotResolveContactOrSendWhenDeliveryIsAlreadyInProgress() {
    var request = request();

    NotificationDeliveryPort deliveryPort =
        new NotificationDeliveryPort() {

          @Override
          public ClaimResult claim(
              CustomerNotificationRequested notificationRequest, Instant claimedAt) {
            assertThat(notificationRequest).isEqualTo(request);
            assertThat(claimedAt).isEqualTo(NOW);

            return new ClaimResult.InProgress(1);
          }

          @Override
          public NotificationResult markSent(
              CustomerNotificationRequested notificationRequest,
              String channel,
              String providerReference,
              int attemptCount,
              Instant sentAt) {
            throw new AssertionError("markSent must not be called for an in-progress delivery");
          }

          @Override
          public NotificationResult markFailed(
              CustomerNotificationRequested notificationRequest,
              String channel,
              int attemptCount,
              String reasonCode,
              Instant failedAt) {
            throw new AssertionError("markFailed must not be called for an in-progress delivery");
          }
        };

    CustomerContactPort customerContactPort =
        customerId -> {
          throw new AssertionError(
              "customer contact must not be resolved for an in-progress delivery");
        };

    NotificationChannelPort notificationChannelPort =
        (notificationRequest, customerContact) -> {
          throw new AssertionError("channel must not be called for an in-progress delivery");
        };

    var handler =
        new NotificationHandler(
            deliveryPort,
            customerContactPort,
            notificationChannelPort,
            Clock.fixed(NOW, ZoneOffset.UTC));

    assertThatThrownBy(() -> handler.handle(request))
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("DELIVERY_ALREADY_IN_PROGRESS");
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
        Instant.parse("2026-09-06T17:59:00Z"),
        "trace-123");
  }

  private static final class FakeNotificationDeliveryPort implements NotificationDeliveryPort {

    private int claimCount;
    private int markSentCount;

    @Override
    public ClaimResult claim(CustomerNotificationRequested request, Instant claimedAt) {
      claimCount++;

      assertThat(request.notificationRequestId()).isEqualTo("notification-123");
      assertThat(claimedAt).isEqualTo(NOW);

      return new ClaimResult.Acquired(1);
    }

    @Override
    public NotificationResult markSent(
        CustomerNotificationRequested request,
        String channel,
        String providerReference,
        int attemptCount,
        Instant sentAt) {

      markSentCount++;

      return new NotificationResult(
          request.schemaVersion(),
          request.notificationRequestId(),
          channel,
          "SENT",
          attemptCount,
          providerReference,
          null,
          sentAt,
          request.traceId());
    }

    @Override
    public NotificationResult markFailed(
        CustomerNotificationRequested request,
        String channel,
        int attemptCount,
        String reasonCode,
        Instant failedAt) {
      throw new AssertionError("markFailed must not be called in the successful flow");
    }
  }
}
