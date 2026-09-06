package com.fraudengine.contracts;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.time.Instant;

@JsonIgnoreProperties(ignoreUnknown = true)
public record TransactionEvent(
    int schemaVersion,
    String eventId,
    String transactionId,
    String customerId,
    long amountMinor,
    String currency,
    @JsonFormat(shape = JsonFormat.Shape.STRING) Instant occurredAt,
    String transactionType,
    String channel,
    String merchantCountry,
    String deviceIdHash,
    String traceId) {}
