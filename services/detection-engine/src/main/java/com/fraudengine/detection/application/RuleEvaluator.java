package com.fraudengine.detection.application;

import com.fraudengine.contracts.TransactionEvent;
import com.fraudengine.detection.domain.assessment.RuleEvaluationStatus;
import com.fraudengine.detection.domain.assessment.RuleResult;
import com.fraudengine.detection.domain.rules.AllCondition;
import com.fraudengine.detection.domain.rules.AmountThresholdCondition;
import com.fraudengine.detection.domain.rules.AnyCondition;
import com.fraudengine.detection.domain.rules.CountWindowCondition;
import com.fraudengine.detection.domain.rules.ExecutableRule;
import com.fraudengine.detection.domain.rules.HistoricalFactsPort;
import com.fraudengine.detection.domain.rules.RuleCondition;
import java.time.Instant;
import java.util.List;
import java.util.Map;

public final class RuleEvaluator {

  private final HistoricalFactsPort historicalFacts;

  public RuleEvaluator(HistoricalFactsPort historicalFacts) {
    this.historicalFacts = historicalFacts;
  }

  public RuleResult evaluate(ExecutableRule rule, TransactionEvent event) {
    ConditionEvaluation evaluation = evaluateCondition(rule.condition(), event);

    return new RuleResult(
        rule.ruleId(),
        rule.ruleVersion(),
        rule.severity(),
        evaluation.status(),
        evaluation.evidenceCodes(),
        evaluation.evaluatedValues());
  }

  private ConditionEvaluation evaluateCondition(RuleCondition condition, TransactionEvent event) {

    if (condition instanceof AmountThresholdCondition amountThreshold) {
      return evaluateAmountThreshold(amountThreshold, event);
    }

    if (condition instanceof CountWindowCondition countWindow) {
      return evaluateCountWindow(countWindow, event);
    }

    if (condition instanceof AnyCondition any) {
      return evaluateAny(any, event);
    }

    if (condition instanceof AllCondition all) {
      return evaluateAll(all, event);
    }

    throw new IllegalArgumentException("UNSUPPORTED_RULE_CONDITION");
  }

  private ConditionEvaluation evaluateAmountThreshold(
      AmountThresholdCondition condition, TransactionEvent event) {

    if (!event.currency().equals(condition.currency())) {
      return new ConditionEvaluation(
          RuleEvaluationStatus.NO_MATCH,
          List.of("CURRENCY_MISMATCH"),
          Map.of(
              "amountMinor", event.amountMinor(),
              "thresholdMinor", condition.amountMinor(),
              "eventCurrency", event.currency(),
              "ruleCurrency", condition.currency()));
    }

    if (event.amountMinor() >= condition.amountMinor()) {
      return new ConditionEvaluation(
          RuleEvaluationStatus.MATCHED,
          List.of("AMOUNT_AT_OR_ABOVE_THRESHOLD"),
          Map.of(
              "amountMinor", event.amountMinor(),
              "thresholdMinor", condition.amountMinor(),
              "currency", event.currency()));
    }

    return new ConditionEvaluation(
        RuleEvaluationStatus.NO_MATCH,
        List.of("AMOUNT_BELOW_THRESHOLD"),
        Map.of(
            "amountMinor", event.amountMinor(),
            "thresholdMinor", condition.amountMinor(),
            "currency", event.currency()));
  }

  private ConditionEvaluation evaluateCountWindow(
      CountWindowCondition condition, TransactionEvent event) {

    Instant from = event.occurredAt().minusSeconds(condition.windowSeconds());

    var facts =
        historicalFacts.load(
            new HistoricalFactsPort.Query(event.customerId(), from, event.occurredAt()));

    if (!facts.available()) {
      return new ConditionEvaluation(
          RuleEvaluationStatus.NOT_EVALUATED,
          List.of("HISTORY_UNAVAILABLE"),
          Map.of(
              "minimumCount", condition.minimumCount(),
              "windowSeconds", condition.windowSeconds()));
    }

    int observedCount = facts.events().size() + 1;

    if (observedCount >= condition.minimumCount()) {
      return new ConditionEvaluation(
          RuleEvaluationStatus.MATCHED,
          List.of("COUNT_AT_OR_ABOVE_MINIMUM"),
          Map.of(
              "observedCount", observedCount,
              "minimumCount", condition.minimumCount(),
              "windowSeconds", condition.windowSeconds()));
    }

    return new ConditionEvaluation(
        RuleEvaluationStatus.NO_MATCH,
        List.of("COUNT_BELOW_MINIMUM"),
        Map.of(
            "observedCount", observedCount,
            "minimumCount", condition.minimumCount(),
            "windowSeconds", condition.windowSeconds()));
  }

  private ConditionEvaluation evaluateAny(AnyCondition condition, TransactionEvent event) {

    boolean hasNotEvaluated = false;
    int evaluatedChildren = 0;

    for (RuleCondition child : condition.children()) {
      ConditionEvaluation childEvaluation = evaluateCondition(child, event);

      evaluatedChildren++;

      if (childEvaluation.status() == RuleEvaluationStatus.MATCHED) {
        return compositeEvaluation(
            RuleEvaluationStatus.MATCHED,
            "ANY_MATCHED",
            evaluatedChildren,
            condition.children().size());
      }

      if (childEvaluation.status() == RuleEvaluationStatus.NOT_EVALUATED) {
        hasNotEvaluated = true;
      }
    }

    if (hasNotEvaluated) {
      return compositeEvaluation(
          RuleEvaluationStatus.NOT_EVALUATED,
          "ANY_NOT_EVALUATED",
          evaluatedChildren,
          condition.children().size());
    }

    return compositeEvaluation(
        RuleEvaluationStatus.NO_MATCH,
        "ANY_NO_MATCH",
        evaluatedChildren,
        condition.children().size());
  }

  private ConditionEvaluation evaluateAll(AllCondition condition, TransactionEvent event) {

    boolean hasNotEvaluated = false;
    int evaluatedChildren = 0;

    for (RuleCondition child : condition.children()) {
      ConditionEvaluation childEvaluation = evaluateCondition(child, event);

      evaluatedChildren++;

      if (childEvaluation.status() == RuleEvaluationStatus.NO_MATCH) {
        return compositeEvaluation(
            RuleEvaluationStatus.NO_MATCH,
            "ALL_NO_MATCH",
            evaluatedChildren,
            condition.children().size());
      }

      if (childEvaluation.status() == RuleEvaluationStatus.NOT_EVALUATED) {
        hasNotEvaluated = true;
      }
    }

    if (hasNotEvaluated) {
      return compositeEvaluation(
          RuleEvaluationStatus.NOT_EVALUATED,
          "ALL_NOT_EVALUATED",
          evaluatedChildren,
          condition.children().size());
    }

    return compositeEvaluation(
        RuleEvaluationStatus.MATCHED,
        "ALL_MATCHED",
        evaluatedChildren,
        condition.children().size());
  }

  private ConditionEvaluation compositeEvaluation(
      RuleEvaluationStatus status, String evidenceCode, int evaluatedChildren, int totalChildren) {

    return new ConditionEvaluation(
        status,
        List.of(evidenceCode),
        Map.of(
            "evaluatedChildren", evaluatedChildren,
            "totalChildren", totalChildren));
  }

  private record ConditionEvaluation(
      RuleEvaluationStatus status,
      List<String> evidenceCodes,
      Map<String, Object> evaluatedValues) {

    private ConditionEvaluation {
      evidenceCodes = List.copyOf(evidenceCodes);
      evaluatedValues = Map.copyOf(evaluatedValues);
    }
  }
}
