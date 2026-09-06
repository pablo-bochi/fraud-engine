package com.fraudengine.contracts;

import com.fasterxml.jackson.annotation.JsonFormat;
import java.time.Instant;

public record InvalidEventReference(
    int schemaVersion,
    String sourceTopic,
    int sourcePartition,
    long sourceOffset,
    String payloadHash,
    Integer sourceSchemaVersion,
    String reasonCode,
    @JsonFormat(shape = JsonFormat.Shape.STRING) Instant detectedAt) {}
