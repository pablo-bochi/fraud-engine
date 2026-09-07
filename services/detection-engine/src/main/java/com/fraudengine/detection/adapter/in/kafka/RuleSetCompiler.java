package com.fraudengine.detection.adapter.in.kafka;

import com.fasterxml.jackson.databind.JsonNode;
import com.fraudengine.contracts.RuleSetSnapshot;
import com.fraudengine.detection.domain.rules.AllCondition;
import com.fraudengine.detection.domain.rules.AmountThresholdCondition;
import com.fraudengine.detection.domain.rules.AnyCondition;
import com.fraudengine.detection.domain.rules.CountWindowCondition;
import com.fraudengine.detection.domain.rules.ExecutableRule;
import com.fraudengine.detection.domain.rules.RuleCondition;
import java.util.Comparator;
import java.util.List;
import java.util.Set;

final class RuleSetCompiler {

  private static final int MAXIMUM_DEPTH = 5;
  private static final int MAXIMUM_WINDOW_SECONDS = 600;
  private static final int MAXIMUM_COMPOSITE_CHILDREN = 20;

  private static final Set<String> SEVERITIES = Set.of("LOW", "MEDIUM", "HIGH", "CRITICAL");
  private final RuleSetSnapshotValidator snapshotValidator = new RuleSetSnapshotValidator();

  CompiledRuleSet compile(RuleSetSnapshot snapshot) {
    snapshotValidator.validate(snapshot);

    List<ExecutableRule> rules =
        snapshot.rules().stream()
            .sorted(Comparator.comparingInt(rule -> rule.path("evaluationOrder").asInt()))
            .map(this::compileRule)
            .toList();

    return new CompiledRuleSet(snapshot, rules);
  }

  private ExecutableRule compileRule(JsonNode rule) {
    if (rule == null || !rule.isObject()) {
      throw new IllegalArgumentException("INVALID_RULE");
    }

    String ruleId = rule.path("ruleId").asText();
    int ruleVersion = rule.path("ruleVersion").asInt(-1);
    String severity = rule.path("severity").asText();

    if (ruleId.isBlank() || ruleVersion < 1 || !SEVERITIES.contains(severity)) {
      throw new IllegalArgumentException("INVALID_RULE");
    }

    return new ExecutableRule(
        ruleId, ruleVersion, severity, compileCondition(rule.path("definition"), 1));
  }

  private RuleCondition compileCondition(JsonNode definition, int depth) {

    if (definition == null || !definition.isObject() || depth > MAXIMUM_DEPTH) {
      throw new IllegalArgumentException("INVALID_RULE_DEFINITION");
    }

    String type = definition.path("type").asText();

    return switch (type) {
      case "AMOUNT_THRESHOLD" -> amountThreshold(definition);

      case "COUNT_WINDOW" -> countWindow(definition);

      case "ALL" -> new AllCondition(compileChildren(definition, depth));

      case "ANY" -> new AnyCondition(compileChildren(definition, depth));

      default -> throw new IllegalArgumentException("UNSUPPORTED_RULE_NODE");
    };
  }

  private AmountThresholdCondition amountThreshold(JsonNode definition) {

    if (!definition.path("amountMinor").canConvertToLong()
        || definition.path("amountMinor").asLong() < 0
        || !definition.path("currency").asText().matches("[A-Z]{3}")) {
      throw new IllegalArgumentException("INVALID_AMOUNT_THRESHOLD");
    }

    return new AmountThresholdCondition(
        definition.path("amountMinor").asLong(), definition.path("currency").asText());
  }

  private CountWindowCondition countWindow(JsonNode definition) {

    int windowSeconds = definition.path("windowSeconds").asInt(-1);

    int minimumCount = definition.path("minimumCount").asInt(-1);

    if (windowSeconds < 1 || windowSeconds > MAXIMUM_WINDOW_SECONDS || minimumCount < 1) {
      throw new IllegalArgumentException("INVALID_COUNT_WINDOW");
    }

    return new CountWindowCondition(windowSeconds, minimumCount);
  }

  private List<RuleCondition> compileChildren(JsonNode definition, int parentDepth) {

    JsonNode children = definition.path("children");

    if (!children.isArray() || children.isEmpty() || children.size() > MAXIMUM_COMPOSITE_CHILDREN) {
      throw new IllegalArgumentException("INVALID_COMPOSITE_RULE");
    }

    return children.valueStream().map(child -> compileCondition(child, parentDepth + 1)).toList();
  }
}
