package com.fraudengine.detection.adapter.in.kafka;

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
  static final String CUSTOMER_HISTORY = "customer-history";
  static final String ACTIVE_RULESETS_GLOBAL_STORE = "active-rulesets-global-store";
  private static final long IDENTITY_RETENTION_MILLIS = 24L * 60 * 60 * 1000;
  private static final JsonSerde<TransactionEvent> TRANSACTION_EVENT_SERDE =
      new JsonSerde<>(TransactionEvent.class);
  private final AtomicReference<CompiledRuleSet> activeRuleset = new AtomicReference<>();
  private final StreamsReadinessHealthIndicator readiness;
  private final DeterministicIdFactory idFactory = new DeterministicIdFactory();

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
            new JsonSerde<>(TransactionIdentityState.class)));

    builder.addStateStore(
        Stores.keyValueStoreBuilder(
            Stores.persistentKeyValueStore(CUSTOMER_DEDUPLICATION),
            Serdes.String(),
            new JsonSerde<>(DeduplicationState.class)));

    builder.addStateStore(
        Stores.keyValueStoreBuilder(
            Stores.persistentKeyValueStore(CUSTOMER_HISTORY),
            Serdes.String(),
            new JsonSerde<>(CustomerHistory.class)));

    builder.addGlobalStore(
        Stores.keyValueStoreBuilder(
            Stores.persistentKeyValueStore(ACTIVE_RULESETS_GLOBAL_STORE),
            Serdes.String(),
            new JsonSerde<>(RuleSetSnapshot.class)),
        RULESET_TOPIC,
        Consumed.with(Serdes.String(), new JsonSerde<>(RuleSetSnapshot.class)),
        () -> new RuleSetUpdateProcessor(activeRuleset, readiness, new RuleSetCompiler()));

    KStream<String, ValidationResult> validated =
        builder.stream(
                TRANSACTION_TOPIC,
                Consumed.with(Serdes.String(), Serdes.ByteArray())
                    .withTimestampExtractor(new TransactionEventTimestampExtractor()))
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

    KStream<String, DeduplicationResult> deduplicated =
        identified
            .filter((key, result) -> !result.conflict())
            .selectKey((key, result) -> result.event().customerId())
            .mapValues(IdentityResult::event)
            .transformValues(() -> new DeduplicationTransformer(), CUSTOMER_DEDUPLICATION);

    deduplicated
        .filter((customerId, result) -> result.conflict())
        .selectKey((customerId, result) -> result.event().transactionId())
        .mapValues(this::eventIdentityConflict)
        .to(
            QUARANTINE_TOPIC,
            Produced.with(Serdes.String(), new JsonSerde<>(QuarantinedEventReference.class)));

    KStream<String, TransactionAssessment> assessments =
        deduplicated
            .filter((customerId, result) -> !result.duplicate() && !result.conflict())
            .mapValues(DeduplicationResult::event)
            .transformValues(
                () -> new CustomerEvaluationTransformer(activeRuleset), CUSTOMER_HISTORY);

    assessments
        .selectKey((customerId, assessment) -> assessment.transactionId())
        .to(
            ASSESSMENT_TOPIC,
            Produced.with(Serdes.String(), new JsonSerde<>(TransactionAssessment.class)));

    KStream<String, InternalAlert> alerts =
        assessments
            .filter((key, assessment) -> "SUSPICIOUS".equals(assessment.status()))
            .mapValues(this::alertFor);

    alerts
        .selectKey((customerId, alert) -> alert.alertId())
        .to(ALERT_TOPIC, Produced.with(Serdes.String(), new JsonSerde<>(InternalAlert.class)));

    alerts
        .mapValues(this::notificationFor)
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

  private QuarantinedEventReference eventIdentityConflict(DeduplicationResult result) {

    TransactionEvent event = result.event();

    return new QuarantinedEventReference(
        1,
        TRANSACTION_TOPIC,
        0,
        -1,
        event.eventId(),
        event.transactionId(),
        result.fingerprint(),
        result.previousFingerprint(),
        "EVENT_IDENTITY_CONFLICT",
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

  private record DeduplicationResult(
      TransactionEvent event,
      String fingerprint,
      String previousFingerprint,
      boolean duplicate,
      boolean conflict) {

    static DeduplicationResult accepted(TransactionEvent event, String fingerprint) {

      return new DeduplicationResult(event, fingerprint, null, false, false);
    }

    static DeduplicationResult duplicate(TransactionEvent event, String fingerprint) {

      return new DeduplicationResult(event, fingerprint, fingerprint, true, false);
    }

    static DeduplicationResult conflict(
        TransactionEvent event, String fingerprint, String previousFingerprint) {

      return new DeduplicationResult(event, fingerprint, previousFingerprint, false, true);
    }
  }

  private static final class IdentityTransformer
      implements ValueTransformerWithKey<String, TransactionEvent, IdentityResult> {
    private KeyValueStore<String, TransactionIdentityState> store;
    private ProcessorContext context;

    @Override
    @SuppressWarnings("unchecked")
    public void init(ProcessorContext context) {
      this.context = context;

      this.store =
          (KeyValueStore<String, TransactionIdentityState>)
              context.getStateStore(TRANSACTION_IDENTITIES);
    }

    @Override
    public IdentityResult transform(String key, TransactionEvent event) {
      long streamTime = currentStreamTime(event);
      TransactionIdentityState seen = store.get(event.transactionId());

      if (seen == null || expired(seen.seenAtStreamTimeMillis(), streamTime)) {
        store.put(event.transactionId(), new TransactionIdentityState(event.eventId(), streamTime));

        return new IdentityResult(event, false);
      }

      return new IdentityResult(event, !seen.eventId().equals(event.eventId()));
    }

    private long currentStreamTime(TransactionEvent event) {
      return Math.max(context.currentStreamTimeMs(), event.occurredAt().toEpochMilli());
    }

    private boolean expired(long seenAt, long streamTime) {
      return streamTime - seenAt >= IDENTITY_RETENTION_MILLIS;
    }

    @Override
    public void close() {}
  }

  private static final class DeduplicationTransformer
      implements ValueTransformerWithKey<String, TransactionEvent, DeduplicationResult> {
    private KeyValueStore<String, DeduplicationState> store;
    private ProcessorContext context;

    @Override
    @SuppressWarnings("unchecked")
    public void init(ProcessorContext context) {
      this.context = context;

      this.store =
          (KeyValueStore<String, DeduplicationState>) context.getStateStore(CUSTOMER_DEDUPLICATION);
    }

    @Override
    public DeduplicationResult transform(String customerId, TransactionEvent event) {
      String key = customerId + ":" + event.eventId();
      String fingerprint = EventFingerprint.of(event);
      long streamTime = currentStreamTime(event);
      DeduplicationState previous = store.get(key);

      if (previous == null || expired(previous.seenAtStreamTimeMillis(), streamTime)) {
        store.put(key, new DeduplicationState(fingerprint, streamTime));
        return DeduplicationResult.accepted(event, fingerprint);
      }

      if (previous.fingerprint().equals(fingerprint)) {
        return DeduplicationResult.duplicate(event, fingerprint);
      }

      return DeduplicationResult.conflict(event, fingerprint, previous.fingerprint());
    }

    private long currentStreamTime(TransactionEvent event) {
      return Math.max(context.currentStreamTimeMs(), event.occurredAt().toEpochMilli());
    }

    private boolean expired(long seenAt, long streamTime) {
      return streamTime - seenAt >= IDENTITY_RETENTION_MILLIS;
    }

    @Override
    public void close() {}
  }

  private InternalAlert alertFor(TransactionAssessment assessment) {
    String alertId = idFactory.alertId(assessment.assessmentId());
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

  private CustomerNotificationRequested notificationFor(InternalAlert alert) {
    String notificationRequestId = idFactory.notificationRequestId(alert.alertId());

    return new CustomerNotificationRequested(
        1,
        notificationRequestId,
        alert.alertId(),
        alert.customerId(),
        alert.transactionId(),
        "FRAUD_SUSPECTED",
        "fraud-suspected-v1",
        "pt-BR",
        alert.createdAt(),
        alert.traceId());
  }
}
