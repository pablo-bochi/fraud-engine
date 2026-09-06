package com.fraudengine.detection.domain.assessment;

import java.util.List;

public record AssessmentAggregation(
    AssessmentStatus status, String finalSeverity, List<RuleResult> matchedRules) {

  public AssessmentAggregation {
    matchedRules = List.copyOf(matchedRules);
  }
}
