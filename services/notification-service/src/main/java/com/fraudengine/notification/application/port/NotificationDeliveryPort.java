package com.fraudengine.notification.application.port;

import com.fraudengine.contracts.CustomerNotificationRequested;
import com.fraudengine.contracts.NotificationResult;
import java.time.Instant;

public interface NotificationDeliveryPort {

  ClaimResult claim(CustomerNotificationRequested request, Instant claimedAt);

  NotificationResult markSent(
      CustomerNotificationRequested request,
      String channel,
      String providerReference,
      int attemptCount,
      Instant sentAt);

  NotificationResult markFailed(
      CustomerNotificationRequested request,
      String channel,
      int attemptCount,
      String reasonCode,
      Instant failedAt);

  sealed interface ClaimResult {

    record Acquired(int attemptCount) implements ClaimResult {}

    record AlreadySent(NotificationResult result) implements ClaimResult {}

    record InProgress(int attemptCount) implements ClaimResult {}
  }
}
