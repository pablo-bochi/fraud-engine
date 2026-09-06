package com.fraudengine.detection.application;

import com.fraudengine.detection.domain.assessment.AssessmentAggregation;
import com.fraudengine.detection.domain.assessment.AssessmentStatus;
import com.fraudengine.detection.domain.assessment.RuleEvaluationStatus;
import com.fraudengine.detection.domain.assessment.RuleResult;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

public final class AssessmentAggregator {

  private static final Map<String, Integer> SEVERITY_RANK =
      Map.of(
          "LOW", 1,
          "MEDIUM", 2,
          "HIGH", 3,
          "CRITICAL", 4);

  public AssessmentAggregation aggregate(List<RuleResult> ruleResults) {

    if (ruleResults == null || ruleResults.isEmpty()) {
      throw new IllegalArgumentException("RULE_RESULTS_MUST_NOT_BE_EMPTY");
    }

    List<RuleResult> matchedRules =
        ruleResults.stream()
            .filter(result -> result.status() == RuleEvaluationStatus.MATCHED)
            .toList();

    if (!matchedRules.isEmpty()) {
      return new AssessmentAggregation(
          AssessmentStatus.SUSPICIOUS, highestSeverity(matchedRules), matchedRules);
    }

    boolean hasNotEvaluated =
        ruleResults.stream()
            .anyMatch(result -> result.status() == RuleEvaluationStatus.NOT_EVALUATED);

    if (hasNotEvaluated) {
      return new AssessmentAggregation(AssessmentStatus.INCONCLUSIVE, null, List.of());
    }

    return new AssessmentAggregation(AssessmentStatus.NOT_SUSPICIOUS, null, List.of());
  }

  private String highestSeverity(List<RuleResult> matchedRules) {

    return matchedRules.stream()
        .map(RuleResult::severity)
        .max(Comparator.comparingInt(this::severityRank))
        .orElseThrow();
  }

  private int severityRank(String severity) {
    Integer rank = SEVERITY_RANK.get(severity);

    if (rank == null) {
      throw new IllegalArgumentException("UNSUPPORTED_SEVERITY: " + severity);
    }

    return rank;
  }
}
