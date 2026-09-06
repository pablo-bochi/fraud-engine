package com.fraudengine.contracts;

import com.fasterxml.jackson.annotation.JsonFormat;
import java.time.Instant;

public record CustomerNotificationRequested(
    int schemaVersion,
    String notificationRequestId,
    String alertId,
    String customerId,
    String transactionId,
    String category,
    String templateId,
    String locale,
    @JsonFormat(shape = JsonFormat.Shape.STRING) Instant requestedAt,
    String traceId) {}
