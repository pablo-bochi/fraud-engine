package com.fraudengine.detection.adapter.in.kafka;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fraudengine.contracts.CustomerNotificationRequested;
import com.fraudengine.contracts.InternalAlert;
import com.fraudengine.contracts.InvalidEventReference;
import com.fraudengine.contracts.QuarantinedEventReference;
import com.fraudengine.contracts.RuleSetSnapshot;
import com.fraudengine.contracts.TransactionAssessment;
import com.fraudengine.contracts.TransactionEvent;
import com.fraudengine.detection.application.DeterministicIdFactory;
import com.fraudengine.detection.health.StreamsReadinessHealthIndicator;
import java.time.Instant;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.ValueTransformerWithKey;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;

public final class DetectionTopology {
  public static final String TRANSACTION_TOPIC = "fraud.transaction.received.v1";
  public static final String RULESET_TOPIC = "fraud.ruleset.active.v1";
  public static final String ASSESSMENT_TOPIC = "fraud.assessment.created.v1";
  public static final String ALERT_TOPIC = "fraud.alert.internal.v1";
  public static final String NOTIFICATION_TOPIC = "fraud.notification.requested.v1";
  public static final String QUARANTINE_TOPIC = "fraud.transaction.quarantine.v1";
  public static final String INVALID_TOPIC = "fraud.transaction.invalid.v1";
  private static final String TRANSACTION_IDENTITIES = "transaction-identities";
  private static final String CUSTOMER_DEDUPLICATION = "customer-deduplication";
  private static final String CUSTOMER_HISTORY = "customer-history";
  static final String ACTIVE_RULESETS_GLOBAL_STORE = "active-rulesets-global-store";
  private static final JsonSerde<TransactionEvent> TRANSACTION_EVENT_SERDE =
      new JsonSerde<>(TransactionEvent.class);
  private final AtomicReference<RuleSetSnapshot> activeRuleset = new AtomicReference<>();
  private final StreamsReadinessHealthIndicator readiness;

  public DetectionTopology() {
    this(new StreamsReadinessHealthIndicator());
  }

  public DetectionTopology(StreamsReadinessHealthIndicator readiness) {
    this.readiness = readiness;
  }

  public Topology build() {
    StreamsBuilder builder = new StreamsBuilder();
    builder.addStateStore(
        Stores.keyValueStoreBuilder(
            Stores.persistentKeyValueStore(TRANSACTION_IDENTITIES),
            Serdes.String(),
            Serdes.String()));
    builder.addStateStore(
        Stores.keyValueStoreBuilder(
            Stores.persistentKeyValueStore(CUSTOMER_DEDUPLICATION),
            Serdes.String(),
            Serdes.String()));
    builder.addStateStore(
        Stores.keyValueStoreBuilder(
            Stores.persistentKeyValueStore(CUSTOMER_HISTORY), Serdes.String(), Serdes.String()));
    builder.addGlobalStore(
        Stores.keyValueStoreBuilder(
            Stores.persistentKeyValueStore(ACTIVE_RULESETS_GLOBAL_STORE),
            Serdes.String(),
            new JsonSerde<>(RuleSetSnapshot.class)),
        RULESET_TOPIC,
        Consumed.with(Serdes.String(), new JsonSerde<>(RuleSetSnapshot.class)),
        () -> new RuleSetUpdateProcessor(activeRuleset, readiness));
    KStream<String, ValidationResult> validated =
        builder.stream(TRANSACTION_TOPIC, Consumed.with(Serdes.String(), Serdes.ByteArray()))
            .mapValues(this::validate);
    validated
        .filter((key, result) -> !result.valid())
        .mapValues(result -> invalid(result.event()))
        .to(
            INVALID_TOPIC,
            Produced.with(Serdes.String(), new JsonSerde<>(InvalidEventReference.class)));
    KStream<String, IdentityResult> identified =
        validated
            .filter((key, result) -> result.valid() && activeRuleset.get() != null)
            .mapValues(ValidationResult::event)
            .transformValues(() -> new IdentityTransformer(), TRANSACTION_IDENTITIES);
    identified
        .filter((key, result) -> result.conflict())
        .mapValues(result -> quarantined(result.event()))
        .to(
            QUARANTINE_TOPIC,
            Produced.with(Serdes.String(), new JsonSerde<>(QuarantinedEventReference.class)));
    KStream<String, TransactionAssessment> assessments =
        identified
            .filter((key, result) -> !result.conflict())
            .selectKey((key, result) -> result.event().customerId())
            .mapValues(IdentityResult::event)
            .transformValues(() -> new DeduplicationTransformer(), CUSTOMER_DEDUPLICATION)
            .filter((key, event) -> event != null)
            .transformValues(() -> new HistoryTransformer(), CUSTOMER_HISTORY)
            .mapValues(this::evaluate);
    assessments
        .selectKey((customerId, assessment) -> assessment.transactionId())
        .to(
            ASSESSMENT_TOPIC,
            Produced.with(Serdes.String(), new JsonSerde<>(TransactionAssessment.class)));
    KStream<String, InternalAlert> alerts =
        assessments
            .filter((key, value) -> "SUSPICIOUS".equals(value.status()))
            .mapValues(this::alertFor);
    alerts
        .selectKey((customerId, alert) -> alert.alertId())
        .to(ALERT_TOPIC, Produced.with(Serdes.String(), new JsonSerde<>(InternalAlert.class)));
    KStream<String, CustomerNotificationRequested> notifications =
        assessments
            .filter((key, value) -> "SUSPICIOUS".equals(value.status()))
            .mapValues(this::notificationFor);
    notifications
        .selectKey((customerId, notification) -> notification.notificationRequestId())
        .to(
            NOTIFICATION_TOPIC,
            Produced.with(Serdes.String(), new JsonSerde<>(CustomerNotificationRequested.class)));
    return builder.build();
  }

  private QuarantinedEventReference quarantined(TransactionEvent event) {
    return new QuarantinedEventReference(
        1,
        TRANSACTION_TOPIC,
        0,
        -1,
        event.eventId(),
        event.transactionId(),
        Integer.toHexString(event.hashCode()),
        "TRANSACTION_IDENTITY_CONFLICT",
        "TRANSACTION_IDENTITY_CONFLICT",
        Instant.now());
  }

  private ValidationResult validate(String key, byte[] payload) {
    try {
      TransactionEvent event =
          TRANSACTION_EVENT_SERDE.deserializer().deserialize(TRANSACTION_TOPIC, payload);
      return new ValidationResult(
          event,
          key != null
              && event != null
              && key.equals(event.transactionId())
              && event.eventId() != null
              && event.customerId() != null
              && event.occurredAt() != null);
    } catch (IllegalArgumentException exception) {
      return new ValidationResult(null, false);
    }
  }

  private InvalidEventReference invalid(TransactionEvent event) {
    return new InvalidEventReference(
        1,
        TRANSACTION_TOPIC,
        0,
        -1,
        Integer.toHexString(event == null ? 0 : event.hashCode()),
        event == null ? null : event.schemaVersion(),
        "INVALID_TRANSACTION_EVENT",
        Instant.now());
  }

  private record ValidationResult(TransactionEvent event, boolean valid) {}

  private record IdentityResult(TransactionEvent event, boolean conflict) {}

  private static final class IdentityTransformer
      implements ValueTransformerWithKey<String, TransactionEvent, IdentityResult> {
    private KeyValueStore<String, String> store;

    @Override
    public void init(ProcessorContext context) {
      store = context.getStateStore(TRANSACTION_IDENTITIES);
    }

    @Override
    public IdentityResult transform(String key, TransactionEvent event) {
      String seen = store.get(event.transactionId());
      if (seen == null) {
        store.put(event.transactionId(), event.eventId());
        return new IdentityResult(event, false);
      }
      return new IdentityResult(event, !seen.equals(event.eventId()));
    }

    @Override
    public void close() {}
  }

  private static final class DeduplicationTransformer
      implements ValueTransformerWithKey<String, TransactionEvent, TransactionEvent> {
    private KeyValueStore<String, String> store;

    @Override
    public void init(ProcessorContext context) {
      store = context.getStateStore(CUSTOMER_DEDUPLICATION);
    }

    @Override
    public TransactionEvent transform(String customerId, TransactionEvent event) {
      String key = customerId + ":" + event.eventId();
      if (store.get(key) != null) return null;
      store.put(key, event.transactionId());
      return event;
    }

    @Override
    public void close() {}
  }

  private record HistoryInput(
      TransactionEvent event, int previousCount, boolean insufficientHistory) {}

  private static final class HistoryTransformer
      implements ValueTransformerWithKey<String, TransactionEvent, HistoryInput> {
    private KeyValueStore<String, String> store;

    @Override
    public void init(ProcessorContext context) {
      store = context.getStateStore(CUSTOMER_HISTORY);
    }

    @Override
    public HistoryInput transform(String customerId, TransactionEvent event) {
      String prior = store.get(customerId);
      String[] timestamps = prior == null || prior.isBlank() ? new String[0] : prior.split(",");
      long from = event.occurredAt().minusSeconds(600).toEpochMilli();
      long latest = Long.MIN_VALUE;
      int count = 0;
      StringBuilder retained = new StringBuilder();
      for (String timestamp : timestamps) {
        long value = Long.parseLong(timestamp);
        latest = Math.max(latest, value);
        if (value >= from) {
          if (retained.length() > 0) retained.append(',');
          retained.append(value);
          if (value <= event.occurredAt().toEpochMilli()) count++;
        }
      }
      if (retained.length() > 0) retained.append(',');
      retained.append(event.occurredAt().toEpochMilli());
      store.put(customerId, retained.toString());
      return new HistoryInput(
          event,
          count,
          latest != Long.MIN_VALUE && event.occurredAt().toEpochMilli() < latest - 900_000);
    }

    @Override
    public void close() {}
  }

  private InternalAlert alertFor(TransactionAssessment assessment) {
    String alertId = new DeterministicIdFactory().alertId(assessment.assessmentId());
    return new InternalAlert(
        1,
        alertId,
        assessment.assessmentId(),
        assessment.eventId(),
        assessment.transactionId(),
        assessment.customerId(),
        assessment.finalSeverity(),
        assessment.ruleResults().stream()
            .filter(result -> "MATCHED".equals(result.path("status").asText()))
            .toList(),
        assessment.rulesetVersion(),
        assessment.evaluatedAt(),
        assessment.traceId());
  }

  private CustomerNotificationRequested notificationFor(TransactionAssessment assessment) {
    String assessmentId = assessment.assessmentId();
    String alertId = new DeterministicIdFactory().alertId(assessmentId);
    return new CustomerNotificationRequested(
        1,
        new DeterministicIdFactory().notificationRequestId(alertId),
        alertId,
        assessment.customerId(),
        assessment.transactionId(),
        "FRAUD_SUSPECTED",
        "fraud-suspected-v1",
        "pt-BR",
        assessment.evaluatedAt(),
        assessment.traceId());
  }

  private TransactionAssessment evaluate(HistoryInput input) {
    TransactionEvent event = input.event();
    RuleSetSnapshot snapshot = activeRuleset.get();
    List<JsonNode> ruleResults =
        snapshot.rules().stream()
            .map(
                rule -> ruleResult(rule, event, input.previousCount(), input.insufficientHistory()))
            .toList();
    List<JsonNode> matchedRules =
        ruleResults.stream()
            .filter(result -> "MATCHED".equals(result.path("status").asText()))
            .toList();
    boolean matched = !matchedRules.isEmpty();
    boolean hasStatefulRule =
        snapshot.rules().stream()
            .anyMatch(rule -> "COUNT_WINDOW".equals(rule.path("definition").path("type").asText()));
    String status =
        matched
            ? "SUSPICIOUS"
            : input.insufficientHistory() && hasStatefulRule ? "INCONCLUSIVE" : "NOT_SUSPICIOUS";
    return new TransactionAssessment(
        1,
        new DeterministicIdFactory().assessmentId(event.eventId()),
        event.eventId(),
        event.transactionId(),
        event.customerId(),
        status,
        matched ? highestSeverity(matchedRules) : null,
        ruleResults,
        snapshot.version(),
        Instant.now(),
        Instant.now(),
        false,
        event.traceId());
  }

  private JsonNode ruleResult(
      JsonNode rule, TransactionEvent event, int previousCount, boolean insufficientHistory) {
    boolean matched = matches(rule, event, previousCount);
    boolean notEvaluated =
        insufficientHistory && "COUNT_WINDOW".equals(rule.path("definition").path("type").asText());
    return JsonNodeFactory.instance
        .objectNode()
        .put("ruleId", rule.path("ruleId").asText())
        .put("ruleVersion", rule.path("ruleVersion").asText())
        .put("severity", rule.path("severity").asText())
        .put("status", notEvaluated ? "NOT_EVALUATED" : matched ? "MATCHED" : "NO_MATCH")
        .put(
            "evidenceCode",
            notEvaluated ? "HISTORY_UNAVAILABLE" : matched ? "RULE_MATCHED" : "RULE_NOT_MATCHED");
  }

  private String highestSeverity(List<JsonNode> matchedRules) {
    return matchedRules.stream()
        .map(result -> result.path("severity").asText())
        .max(Comparator.comparingInt(this::severityRank))
        .orElseThrow();
  }

  private int severityRank(String severity) {
    return switch (severity) {
      case "LOW" -> 1;
      case "MEDIUM" -> 2;
      case "HIGH" -> 3;
      case "CRITICAL" -> 4;
      default -> throw new IllegalArgumentException("UNSUPPORTED_SEVERITY");
    };
  }

  private boolean matches(JsonNode rule, TransactionEvent event, int previousCount) {
    JsonNode definition = rule.path("definition");
    if ("COUNT_WINDOW".equals(definition.path("type").asText())) {
      return previousCount + 1 >= definition.path("minimumCount").asInt(Integer.MAX_VALUE);
    }
    return "AMOUNT_THRESHOLD".equals(definition.path("type").asText())
        && event.currency().equals(definition.path("currency").asText())
        && event.amountMinor() >= definition.path("amountMinor").asLong(Long.MAX_VALUE);
  }

  static boolean valid(RuleSetSnapshot snapshot) {
    return snapshot != null
        && snapshot.version() > 0
        && snapshot.contentHash() != null
        && !snapshot.contentHash().isBlank()
        && snapshot.rules() != null
        && !snapshot.rules().isEmpty();
  }
}
