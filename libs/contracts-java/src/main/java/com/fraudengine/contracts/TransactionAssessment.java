package com.fraudengine.contracts;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.databind.JsonNode;
import java.time.Instant;
import java.util.List;

public record TransactionAssessment(
    int schemaVersion,
    String assessmentId,
    String eventId,
    String transactionId,
    String customerId,
    String status,
    String finalSeverity,
    List<JsonNode> ruleResults,
    long rulesetVersion,
    @JsonFormat(shape = JsonFormat.Shape.STRING) Instant receivedAt,
    @JsonFormat(shape = JsonFormat.Shape.STRING) Instant evaluatedAt,
    boolean late,
    String traceId) {}
