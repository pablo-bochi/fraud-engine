package com.fraudengine.detection.domain.rules;

public sealed interface RuleCondition
    permits AmountThresholdCondition, CountWindowCondition, AnyCondition, AllCondition {}
