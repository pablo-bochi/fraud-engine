package com.fraudengine.detection.adapter.in.kafka;

import com.fraudengine.contracts.RuleSetSnapshot;
import com.fraudengine.detection.domain.rules.ExecutableRule;
import java.util.List;

record CompiledRuleSet(RuleSetSnapshot snapshot, List<ExecutableRule> rules) {

  CompiledRuleSet {
    rules = List.copyOf(rules);
  }
}
