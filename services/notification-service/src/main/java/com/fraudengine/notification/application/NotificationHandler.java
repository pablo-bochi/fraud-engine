package com.fraudengine.notification.application;

import com.fraudengine.contracts.CustomerNotificationRequested;
import com.fraudengine.contracts.NotificationResult;
import com.fraudengine.notification.application.port.CustomerContactPort;
import com.fraudengine.notification.application.port.NotificationChannelPort;
import com.fraudengine.notification.application.port.NotificationDeliveryPort;
import com.fraudengine.notification.application.port.NotificationDeliveryPort.ClaimResult;
import java.time.Clock;
import java.time.Instant;
import java.util.Objects;

public final class NotificationHandler {

  private static final String EMAIL_CHANNEL = "EMAIL";
  private static final String CONTACT_RESOLUTION_FAILED = "CONTACT_RESOLUTION_FAILED";
  private static final String CHANNEL_DELIVERY_FAILED = "CHANNEL_DELIVERY_FAILED";
  private static final String DELIVERY_ALREADY_IN_PROGRESS = "DELIVERY_ALREADY_IN_PROGRESS";

  private final NotificationDeliveryPort deliveryPort;
  private final CustomerContactPort customerContactPort;
  private final NotificationChannelPort notificationChannelPort;
  private final Clock clock;

  public NotificationHandler(
      NotificationDeliveryPort deliveryPort,
      CustomerContactPort customerContactPort,
      NotificationChannelPort notificationChannelPort,
      Clock clock) {
    this.deliveryPort = Objects.requireNonNull(deliveryPort);
    this.customerContactPort = Objects.requireNonNull(customerContactPort);
    this.notificationChannelPort = Objects.requireNonNull(notificationChannelPort);
    this.clock = Objects.requireNonNull(clock);
  }

  public NotificationResult handle(CustomerNotificationRequested request) {
    Instant now = clock.instant();

    ClaimResult claimResult = deliveryPort.claim(request, now);

    if (claimResult instanceof ClaimResult.AlreadySent alreadySent) {
      return alreadySent.result();
    }

    if (claimResult instanceof ClaimResult.InProgress) {
      throw new IllegalStateException(DELIVERY_ALREADY_IN_PROGRESS);
    }

    if (claimResult instanceof ClaimResult.Acquired acquired) {
      return handleAcquired(request, acquired, now);
    }

    throw new IllegalStateException("Unsupported notification claim result");
  }

  private NotificationResult handleAcquired(
      CustomerNotificationRequested request, ClaimResult.Acquired acquired, Instant now) {

    CustomerContactPort.CustomerContact contact;

    try {
      contact = customerContactPort.findByCustomerId(request.customerId());
    } catch (RuntimeException exception) {
      return deliveryPort.markFailed(
          request,
          EMAIL_CHANNEL,
          acquired.attemptCount(),
          CONTACT_RESOLUTION_FAILED,
          now);
    }

    try {
      var channelResult = notificationChannelPort.send(request, contact);

      return deliveryPort.markSent(
          request,
          channelResult.channel(),
          channelResult.providerReference(),
          acquired.attemptCount(),
          now);
    } catch (RuntimeException exception) {
      return deliveryPort.markFailed(
          request,
          EMAIL_CHANNEL,
          acquired.attemptCount(),
          CHANNEL_DELIVERY_FAILED,
          now);
    }
  }
}
