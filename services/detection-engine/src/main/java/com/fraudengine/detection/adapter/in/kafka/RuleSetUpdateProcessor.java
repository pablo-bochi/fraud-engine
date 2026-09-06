package com.fraudengine.detection.adapter.in.kafka;

import com.fraudengine.contracts.RuleSetSnapshot;
import com.fraudengine.detection.health.StreamsReadinessHealthIndicator;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.Record;

final class RuleSetUpdateProcessor implements Processor<String, RuleSetSnapshot, Void, Void> {
  private final AtomicReference<RuleSetSnapshot> activeRuleset;
  private final StreamsReadinessHealthIndicator readiness;

  RuleSetUpdateProcessor(
      AtomicReference<RuleSetSnapshot> activeRuleset, StreamsReadinessHealthIndicator readiness) {
    this.activeRuleset = activeRuleset;
    this.readiness = readiness;
  }

  @Override
  public void process(Record<String, RuleSetSnapshot> record) {
    RuleSetSnapshot candidate = record.value();
    if (!"ACTIVE".equals(record.key()) || !DetectionTopology.valid(candidate)) return;
    activeRuleset.updateAndGet(
        current -> {
          if (current == null || candidate.version() > current.version()) {
            readiness.markRulesetLoaded(candidate.version());
            return candidate;
          }
          return current;
        });
  }
}
