package com.fraudengine.detection.domain.rules;

import java.util.List;

public record AnyCondition(List<RuleCondition> children) implements RuleCondition {}
