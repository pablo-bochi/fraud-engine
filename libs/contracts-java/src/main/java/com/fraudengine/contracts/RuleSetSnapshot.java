package com.fraudengine.contracts;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.databind.JsonNode;
import java.time.Instant;
import java.util.List;

public record RuleSetSnapshot(
    int schemaVersion,
    String snapshotId,
    long version,
    String contentHash,
    String approvedChangeRuleVersionId,
    List<JsonNode> rules,
    @JsonFormat(shape = JsonFormat.Shape.STRING) Instant createdAt) {}
