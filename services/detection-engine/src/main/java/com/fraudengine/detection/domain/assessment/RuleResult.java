package com.fraudengine.detection.domain.assessment;

import java.util.List;
import java.util.Map;

public record RuleResult(
    String ruleId,
    int ruleVersion,
    String severity,
    RuleEvaluationStatus status,
    List<String> evidenceCodes,
    Map<String, Object> evaluatedValues) {

  public RuleResult {
    evidenceCodes = List.copyOf(evidenceCodes);
    evaluatedValues = Map.copyOf(evaluatedValues);
  }
}
