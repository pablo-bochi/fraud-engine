package com.fraudengine.detection.domain.rules;

public record ExecutableRule(
    String ruleId, int ruleVersion, String severity, RuleCondition condition) {}
