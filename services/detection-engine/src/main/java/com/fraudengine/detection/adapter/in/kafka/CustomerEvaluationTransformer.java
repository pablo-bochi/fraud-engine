package com.fraudengine.detection.adapter.in.kafka;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fraudengine.contracts.TransactionAssessment;
import com.fraudengine.contracts.TransactionEvent;
import com.fraudengine.detection.application.AssessmentAggregator;
import com.fraudengine.detection.application.DeterministicIdFactory;
import com.fraudengine.detection.application.RuleEvaluator;
import com.fraudengine.detection.domain.assessment.AssessmentAggregation;
import com.fraudengine.detection.domain.assessment.RuleResult;
import com.fraudengine.detection.domain.rules.HistoricalFactsPort;
import io.micrometer.core.instrument.Timer;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.kafka.streams.kstream.ValueTransformerWithKey;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;

final class CustomerEvaluationTransformer
    implements ValueTransformerWithKey<String, TransactionEvent, TransactionAssessment> {

  private static final long HISTORY_RETENTION_MILLIS = 15 * 60 * 1000L;

  private static final ObjectMapper MAPPER = new ObjectMapper().findAndRegisterModules();

  private final AtomicReference<CompiledRuleSet> activeRuleset;

  private final AssessmentAggregator aggregator = new AssessmentAggregator();

  private final DeterministicIdFactory idFactory = new DeterministicIdFactory();

  private KeyValueStore<String, CustomerHistory> historyStore;

  private ProcessorContext context;

  private final Timer evaluationTimer;

  CustomerEvaluationTransformer(AtomicReference<CompiledRuleSet> activeRuleset) {

    this(activeRuleset, null);
  }

  CustomerEvaluationTransformer(
      AtomicReference<CompiledRuleSet> activeRuleset, Timer evaluationTimer) {

    this.activeRuleset = activeRuleset;
    this.evaluationTimer = evaluationTimer;
  }

  @Override
  @SuppressWarnings("unchecked")
  public void init(ProcessorContext context) {
    this.context = context;

    this.historyStore =
        (KeyValueStore<String, CustomerHistory>)
            context.getStateStore(DetectionTopology.CUSTOMER_HISTORY);
  }

  @Override
  public TransactionAssessment transform(String customerId, TransactionEvent event) {
    long startedAt = System.nanoTime();
    CompiledRuleSet ruleset = activeRuleset.get();

    if (ruleset == null) {
      throw new IllegalStateException("NO_VALID_RULESET_LOADED");
    }

    long eventTime = event.occurredAt().toEpochMilli();

    long currentStreamTime = context.currentStreamTimeMs();

    boolean late = currentStreamTime >= 0 && eventTime < currentStreamTime;

    long streamTime = Math.max(currentStreamTime, eventTime);

    long retentionCutoff = streamTime - HISTORY_RETENTION_MILLIS;

    CustomerHistory storedHistory = historyStore.get(customerId);

    List<TransactionEvent> retainedHistory =
        storedHistory == null
            ? List.of()
            : storedHistory.events().stream()
                .filter(
                    historicalEvent ->
                        historicalEvent.occurredAt().toEpochMilli() >= retentionCutoff)
                .toList();

    boolean historyAvailable = event.occurredAt().toEpochMilli() >= retentionCutoff;

    HistoricalFactsPort historicalFacts =
        query ->
            new HistoricalFactsPort.HistoricalFacts(
                historyAvailable,
                retainedHistory.stream()
                    .filter(
                        historicalEvent ->
                            !historicalEvent.occurredAt().isBefore(query.from())
                                && !historicalEvent.occurredAt().isAfter(query.until()))
                    .toList());

    RuleEvaluator evaluator = new RuleEvaluator(historicalFacts);

    List<RuleResult> ruleResults =
        ruleset.rules().stream().map(rule -> evaluator.evaluate(rule, event)).toList();

    AssessmentAggregation aggregation = aggregator.aggregate(ruleResults);

    updateHistory(customerId, event, retainedHistory, retentionCutoff);

    List<JsonNode> serializedRuleResults =
        ruleResults.stream().map(result -> (JsonNode) MAPPER.valueToTree(result)).toList();

    Instant evaluatedAt = Instant.now();

    TransactionAssessment assessment =
        new TransactionAssessment(
            1,
            idFactory.assessmentId(event.eventId()),
            event.eventId(),
            event.transactionId(),
            event.customerId(),
            aggregation.status().name(),
            aggregation.finalSeverity(),
            serializedRuleResults,
            ruleset.snapshot().version(),
            evaluatedAt,
            evaluatedAt,
            late,
            event.traceId());

    if (evaluationTimer != null) {
      evaluationTimer.record(System.nanoTime() - startedAt, TimeUnit.NANOSECONDS);
    }

    return assessment;
  }

  private void updateHistory(
      String customerId,
      TransactionEvent currentEvent,
      List<TransactionEvent> retainedHistory,
      long retentionCutoff) {

    List<TransactionEvent> updated = new ArrayList<>(retainedHistory);

    if (currentEvent.occurredAt().toEpochMilli() >= retentionCutoff) {
      updated.add(currentEvent);
    }

    historyStore.put(customerId, new CustomerHistory(updated));
  }

  @Override
  public void close() {}
}
