package com.fraudengine.detection.domain.rules;

public record CountWindowCondition(int windowSeconds, int minimumCount) implements RuleCondition {}
