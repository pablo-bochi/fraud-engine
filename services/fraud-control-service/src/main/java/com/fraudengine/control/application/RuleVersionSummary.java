package com.fraudengine.control.application;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.UUID;

public record RuleVersionSummary(
    UUID ruleVersionId,
    int versionNumber,
    String changeType,
    String status,
    JsonNode definition,
    String authorSubject,
    String approverSubject) {}
