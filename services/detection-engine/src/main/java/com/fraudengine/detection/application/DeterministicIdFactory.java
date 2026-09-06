package com.fraudengine.detection.application;

import java.nio.charset.StandardCharsets;
import java.util.UUID;

public final class DeterministicIdFactory {

  private static final String ASSESSMENT_NAMESPACE = "ASSESSMENT";
  private static final String ALERT_NAMESPACE = "ALERT";
  private static final String NOTIFICATION_REQUEST_NAMESPACE = "NOTIFICATION-REQUEST";

  public String assessmentId(String eventId) {
    return buildDeterministicId(ASSESSMENT_NAMESPACE, eventId);
  }

  public String alertId(String assessmentId) {
    return buildDeterministicId(ALERT_NAMESPACE, assessmentId);
  }

  public String notificationRequestId(String alertId) {
    return buildDeterministicId(NOTIFICATION_REQUEST_NAMESPACE, alertId);
  }

  private String buildDeterministicId(String namespace, String id) {

    if (id == null || id.isBlank()) {
      throw new IllegalArgumentException("ID_MUST_NOT_BE_BLANK");
    }

    String combined = namespace + ":" + id;

    return UUID.nameUUIDFromBytes(combined.getBytes(StandardCharsets.UTF_8)).toString();
  }
}
