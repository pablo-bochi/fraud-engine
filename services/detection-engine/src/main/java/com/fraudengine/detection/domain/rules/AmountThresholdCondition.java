package com.fraudengine.detection.domain.rules;

public record AmountThresholdCondition(long amountMinor, String currency)
    implements RuleCondition {}
