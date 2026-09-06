package com.fraudengine.control.application;

import java.time.Instant;
import java.util.UUID;

public record OutboxEventSummary(
    UUID outboxEventId,
    long aggregateVersion,
    String status,
    int attemptCount,
    Instant nextAttemptAt,
    String lastErrorCode) {}
