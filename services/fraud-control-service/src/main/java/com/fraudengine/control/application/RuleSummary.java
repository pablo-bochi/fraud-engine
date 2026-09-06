package com.fraudengine.control.application;

import java.time.Instant;
import java.util.UUID;

public record RuleSummary(
    UUID ruleId, String ruleKey, String name, String createdBySubject, Instant createdAt) {}
