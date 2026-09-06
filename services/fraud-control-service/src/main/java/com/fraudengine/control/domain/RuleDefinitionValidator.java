package com.fraudengine.control.domain;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.Set;

public final class RuleDefinitionValidator {
  private static final Set<String> LEAF_TYPES =
      Set.of("AMOUNT_THRESHOLD", "ATTRIBUTE_COMPARISON", "COUNT_WINDOW", "SUM_WINDOW");
  private static final Set<String> COMPOSITE_TYPES = Set.of("ALL", "ANY");
  private static final Set<String> ATTRIBUTE_OPERATORS = Set.of("EQ", "NEQ", "IN");
  private static final int MAXIMUM_COMPOSITE_CHILDREN = 20;
  private final int maximumDepth;
  private final int maximumWindowSeconds;

  public RuleDefinitionValidator(int maximumDepth, int maximumWindowSeconds) {
    this.maximumDepth = maximumDepth;
    this.maximumWindowSeconds = maximumWindowSeconds;
  }

  public void validate(JsonNode definition) {
    validate(definition, 1);
  }

  private void validate(JsonNode node, int depth) {
    if (node == null || !node.isObject() || !node.hasNonNull("type")) {
      throw new IllegalArgumentException("INVALID_RULE_DEFINITION");
    }
    if (depth > maximumDepth) {
      throw new IllegalArgumentException("RULE_DEPTH_EXCEEDS_LIMIT");
    }
    String type = node.get("type").asText();
    if (LEAF_TYPES.contains(type)) {
      validateLeaf(type, node);
      return;
    }
    if (COMPOSITE_TYPES.contains(type)) {
      JsonNode children = node.get("children");
      if (!children.isArray() || children.isEmpty()) {
        throw new IllegalArgumentException("INVALID_COMPOSITE_RULE");
      }
      if (children.size() > MAXIMUM_COMPOSITE_CHILDREN) {
        throw new IllegalArgumentException("COMPOSITE_WIDTH_EXCEEDS_LIMIT");
      }
      children.forEach(child -> validate(child, depth + 1));
      return;
    }
    throw new IllegalArgumentException("UNSUPPORTED_RULE_NODE");
  }

  private void validateLeaf(String type, JsonNode node) {
    if (type.equals("AMOUNT_THRESHOLD")) {
      if (!node.path("amountMinor").canConvertToLong()
          || node.path("amountMinor").asLong() < 0
          || !node.path("currency").asText().matches("[A-Z]{3}")) {
        throw new IllegalArgumentException("INVALID_AMOUNT_THRESHOLD");
      }
    }
    if (type.equals("ATTRIBUTE_COMPARISON")) {
      if (!node.path("attribute").isTextual()
          || !node.path("attribute").asText().matches("[A-Za-z][A-Za-z0-9_.]{0,99}")
          || !ATTRIBUTE_OPERATORS.contains(node.path("operator").asText())
          || node.path("value").isMissingNode()
          || node.path("value").isContainerNode()) {
        throw new IllegalArgumentException("INVALID_ATTRIBUTE_COMPARISON");
      }
    }
    if (type.equals("COUNT_WINDOW") || type.equals("SUM_WINDOW")) {
      int seconds = node.path("windowSeconds").asInt(-1);
      if (seconds < 1) {
        throw new IllegalArgumentException("INVALID_WINDOW");
      }
      if (seconds > maximumWindowSeconds) {
        throw new IllegalArgumentException("WINDOW_EXCEEDS_LIMIT");
      }
      if (type.equals("COUNT_WINDOW")
          && (!node.path("minimumCount").canConvertToInt()
              || node.path("minimumCount").asInt() < 1)) {
        throw new IllegalArgumentException("INVALID_COUNT_WINDOW");
      }
      if (type.equals("SUM_WINDOW")
          && (!node.path("minimumAmountMinor").canConvertToLong()
              || node.path("minimumAmountMinor").asLong() < 1)) {
        throw new IllegalArgumentException("INVALID_SUM_WINDOW");
      }
    }
  }
}
