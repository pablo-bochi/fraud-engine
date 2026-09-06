package com.fraudengine.contracts;

import com.fasterxml.jackson.annotation.JsonFormat;
import java.time.Instant;

public record QuarantinedEventReference(
    int schemaVersion,
    String sourceTopic,
    int sourcePartition,
    long sourceOffset,
    String eventId,
    String transactionId,
    String payloadHash,
    String conflictingIdentityHash,
    String reasonCode,
    @JsonFormat(shape = JsonFormat.Shape.STRING) Instant detectedAt) {}
