package com.fraudengine.contracts;

import com.fasterxml.jackson.annotation.JsonFormat;
import java.time.Instant;

public record NotificationResult(
    int schemaVersion,
    String notificationRequestId,
    String channel,
    String status,
    int attemptCount,
    String providerReference,
    String reasonCode,
    @JsonFormat(shape = JsonFormat.Shape.STRING) Instant updatedAt,
    String traceId) {}
