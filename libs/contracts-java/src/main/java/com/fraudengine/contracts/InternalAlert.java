package com.fraudengine.contracts;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.databind.JsonNode;
import java.time.Instant;
import java.util.List;

public record InternalAlert(
    int schemaVersion,
    String alertId,
    String assessmentId,
    String eventId,
    String transactionId,
    String customerId,
    String severity,
    List<JsonNode> matchedRules,
    long rulesetVersion,
    @JsonFormat(shape = JsonFormat.Shape.STRING) Instant createdAt,
    String traceId) {}
