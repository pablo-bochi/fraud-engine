package com.fraudengine.detection.domain.rules;

import java.util.List;

public record AllCondition(List<RuleCondition> children) implements RuleCondition {}
