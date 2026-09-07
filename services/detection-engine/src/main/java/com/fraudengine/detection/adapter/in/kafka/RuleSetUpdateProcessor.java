package com.fraudengine.detection.adapter.in.kafka;

import com.fraudengine.contracts.RuleSetSnapshot;
import com.fraudengine.detection.health.StreamsReadinessHealthIndicator;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;

final class RuleSetUpdateProcessor implements Processor<String, RuleSetSnapshot, Void, Void> {

  private final AtomicReference<CompiledRuleSet> activeRuleset;

  private final StreamsReadinessHealthIndicator readiness;

  private final RuleSetCompiler compiler;

  private KeyValueStore<String, RuleSetSnapshot> store;

  RuleSetUpdateProcessor(
      AtomicReference<CompiledRuleSet> activeRuleset,
      StreamsReadinessHealthIndicator readiness,
      RuleSetCompiler compiler) {
    this.activeRuleset = activeRuleset;
    this.readiness = readiness;
    this.compiler = compiler;
  }

  @Override
  public void init(ProcessorContext<Void, Void> context) {

    store = context.getStateStore(DetectionTopology.ACTIVE_RULESETS_GLOBAL_STORE);
  }

  @Override
  public void process(Record<String, RuleSetSnapshot> record) {

    if (!"ACTIVE".equals(record.key())) {
      return;
    }

    CompiledRuleSet candidate;

    try {
      candidate = compiler.compile(record.value());
    } catch (IllegalArgumentException invalidRuleset) {
      return;
    }

    CompiledRuleSet current = activeRuleset.get();

    if (current != null && candidate.snapshot().version() <= current.snapshot().version()) {
      return;
    }

    store.put("ACTIVE", candidate.snapshot());

    activeRuleset.set(candidate);

    readiness.markRulesetLoaded(candidate.snapshot().version());
  }
}
