package com.fraudengine.detection.application;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fraudengine.detection.domain.assessment.AssessmentStatus;
import com.fraudengine.detection.domain.assessment.RuleEvaluationStatus;
import com.fraudengine.detection.domain.assessment.RuleResult;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class AssessmentAggregatorTest {

  private final AssessmentAggregator aggregator = new AssessmentAggregator();

  @Test
  void anyMatchedRuleMakesAssessmentSuspicious() {
    RuleResult noMatch = result("rule-1", "HIGH", RuleEvaluationStatus.NO_MATCH);

    RuleResult matched = result("rule-2", "MEDIUM", RuleEvaluationStatus.MATCHED);

    var aggregation = aggregator.aggregate(List.of(noMatch, matched));

    assertThat(aggregation.status()).isEqualTo(AssessmentStatus.SUSPICIOUS);

    assertThat(aggregation.matchedRules()).containsExactly(matched);

    assertThat(aggregation.finalSeverity()).isEqualTo("MEDIUM");
  }

  @Test
  void usesHighestSeverityAmongMatchedRules() {
    RuleResult medium = result("rule-medium", "MEDIUM", RuleEvaluationStatus.MATCHED);

    RuleResult critical = result("rule-critical", "CRITICAL", RuleEvaluationStatus.MATCHED);

    RuleResult highNoMatch = result("rule-high", "HIGH", RuleEvaluationStatus.NO_MATCH);

    var aggregation = aggregator.aggregate(List.of(medium, critical, highNoMatch));

    assertThat(aggregation.status()).isEqualTo(AssessmentStatus.SUSPICIOUS);

    assertThat(aggregation.finalSeverity()).isEqualTo("CRITICAL");

    assertThat(aggregation.matchedRules()).containsExactly(medium, critical);
  }

  @Test
  void matchedRuleWinsOverNotEvaluatedRule() {
    RuleResult unavailable =
        result("rule-unavailable", "CRITICAL", RuleEvaluationStatus.NOT_EVALUATED);

    RuleResult matched = result("rule-matched", "LOW", RuleEvaluationStatus.MATCHED);

    var aggregation = aggregator.aggregate(List.of(unavailable, matched));

    assertThat(aggregation.status()).isEqualTo(AssessmentStatus.SUSPICIOUS);

    assertThat(aggregation.finalSeverity()).isEqualTo("LOW");
  }

  @Test
  void noMatchPlusNotEvaluatedIsInconclusive() {
    var aggregation =
        aggregator.aggregate(
            List.of(
                result("rule-1", "HIGH", RuleEvaluationStatus.NO_MATCH),
                result("rule-2", "MEDIUM", RuleEvaluationStatus.NOT_EVALUATED)));

    assertThat(aggregation.status()).isEqualTo(AssessmentStatus.INCONCLUSIVE);

    assertThat(aggregation.finalSeverity()).isNull();

    assertThat(aggregation.matchedRules()).isEmpty();
  }

  @Test
  void allNoMatchIsNotSuspicious() {
    var aggregation =
        aggregator.aggregate(
            List.of(
                result("rule-1", "HIGH", RuleEvaluationStatus.NO_MATCH),
                result("rule-2", "MEDIUM", RuleEvaluationStatus.NO_MATCH)));

    assertThat(aggregation.status()).isEqualTo(AssessmentStatus.NOT_SUSPICIOUS);

    assertThat(aggregation.finalSeverity()).isNull();

    assertThat(aggregation.matchedRules()).isEmpty();
  }

  @Test
  void rejectsEmptyRuleResults() {
    assertThatThrownBy(() -> aggregator.aggregate(List.of()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("RULE_RESULTS_MUST_NOT_BE_EMPTY");
  }

  private RuleResult result(String ruleId, String severity, RuleEvaluationStatus status) {

    return new RuleResult(ruleId, 1, severity, status, List.of(), Map.of());
  }
}
