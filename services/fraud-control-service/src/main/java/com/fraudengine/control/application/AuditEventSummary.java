package com.fraudengine.control.application;

import java.time.Instant;
import java.util.UUID;

public record AuditEventSummary(
    UUID auditEventId,
    String actorSubject,
    String action,
    String outcome,
    String entityType,
    String entityId,
    Instant occurredAt) {}
