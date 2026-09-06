package com.fraudengine.detection.domain.rules;

import static org.assertj.core.api.Assertions.assertThat;

import com.fraudengine.contracts.TransactionEvent;
import com.fraudengine.detection.application.RuleEvaluator;
import com.fraudengine.detection.domain.assessment.RuleEvaluationStatus;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class RuleEvaluatorTest {

  @Test
  void amountBelowThresholdDoesNotMatch() {
    var rule =
        new ExecutableRule("rule-amount", 1, "HIGH", new AmountThresholdCondition(10_000L, "BRL"));

    var event =
        new TransactionEvent(
            1,
            "event-001",
            "transaction-001",
            "customer-001",
            9_999L,
            "BRL",
            Instant.parse("2026-09-06T12:00:00Z"),
            "PURCHASE",
            "ECOMMERCE",
            "BR",
            "device-hash",
            "trace-001");

    HistoricalFactsPort history =
        query -> {
          throw new AssertionError("Amount threshold must not query historical state");
        };

    var evaluator = new RuleEvaluator(history);

    var result = evaluator.evaluate(rule, event);

    assertThat(result.status()).isEqualTo(RuleEvaluationStatus.NO_MATCH);
  }

  @Test
  void amountExactlyAtThresholdMatches() {
    var rule =
        new ExecutableRule("rule-amount", 1, "HIGH", new AmountThresholdCondition(10_000L, "BRL"));

    var event =
        new TransactionEvent(
            1,
            "event-001",
            "transaction-001",
            "customer-001",
            10_000L,
            "BRL",
            Instant.parse("2026-09-06T12:00:00Z"),
            "PURCHASE",
            "ECOMMERCE",
            "BR",
            "device-hash",
            "trace-001");

    HistoricalFactsPort history =
        query -> {
          throw new AssertionError("Amount threshold must not query historical state");
        };

    var result = new RuleEvaluator(history).evaluate(rule, event);

    assertThat(result.status()).isEqualTo(RuleEvaluationStatus.MATCHED);
  }

  @Test
  void countWindowIncludesCurrentEventAndQueriesOnlyItsCustomerWithinTheWindow() {
    var rule = new ExecutableRule("rule-count", 1, "MEDIUM", new CountWindowCondition(600, 3));

    var occurredAt = Instant.parse("2026-09-06T12:00:00Z");

    var event =
        new TransactionEvent(
            1,
            "event-current",
            "transaction-current",
            "customer-001",
            1_000L,
            "BRL",
            occurredAt,
            "PURCHASE",
            "ECOMMERCE",
            "BR",
            "device-hash",
            "trace-001");

    var previousEvent1 =
        new TransactionEvent(
            1,
            "event-previous-1",
            "transaction-previous-1",
            "customer-001",
            500L,
            "BRL",
            occurredAt.minusSeconds(100),
            "PURCHASE",
            "ECOMMERCE",
            "BR",
            "device-hash",
            "trace-previous-1");

    var previousEvent2 =
        new TransactionEvent(
            1,
            "event-previous-2",
            "transaction-previous-2",
            "customer-001",
            700L,
            "BRL",
            occurredAt.minusSeconds(300),
            "PURCHASE",
            "ECOMMERCE",
            "BR",
            "device-hash",
            "trace-previous-2");

    HistoricalFactsPort history =
        query -> {
          assertThat(query.customerId()).isEqualTo("customer-001");
          assertThat(query.from()).isEqualTo(occurredAt.minusSeconds(600));
          assertThat(query.until()).isEqualTo(occurredAt);

          return new HistoricalFactsPort.HistoricalFacts(
              true, List.of(previousEvent1, previousEvent2));
        };

    var result = new RuleEvaluator(history).evaluate(rule, event);

    assertThat(result.status()).isEqualTo(RuleEvaluationStatus.MATCHED);
  }

  @Test
  void countWindowIsNotEvaluatedWhenRequiredHistoryIsUnavailable() {
    var rule = new ExecutableRule("rule-count", 1, "MEDIUM", new CountWindowCondition(600, 3));

    var occurredAt = Instant.parse("2026-09-06T12:00:00Z");

    var event =
        new TransactionEvent(
            1,
            "event-current",
            "transaction-current",
            "customer-001",
            1_000L,
            "BRL",
            occurredAt,
            "PURCHASE",
            "ECOMMERCE",
            "BR",
            "device-hash",
            "trace-001");

    HistoricalFactsPort history =
        query -> new HistoricalFactsPort.HistoricalFacts(false, List.of());

    var result = new RuleEvaluator(history).evaluate(rule, event);

    assertThat(result.status()).isEqualTo(RuleEvaluationStatus.NOT_EVALUATED);
  }

  @Test
  void anyIsNotEvaluatedWhenNoChildMatchesAndOneChildCannotBeEvaluated() {
    var rule =
        new ExecutableRule(
            "rule-any",
            1,
            "HIGH",
            new AnyCondition(
                List.of(
                    new AmountThresholdCondition(10_000L, "BRL"),
                    new CountWindowCondition(600, 3))));

    var occurredAt = Instant.parse("2026-09-06T12:00:00Z");

    var event =
        new TransactionEvent(
            1,
            "event-current",
            "transaction-current",
            "customer-001",
            5_000L,
            "BRL",
            occurredAt,
            "PURCHASE",
            "ECOMMERCE",
            "BR",
            "device-hash",
            "trace-001");

    HistoricalFactsPort history =
        query -> new HistoricalFactsPort.HistoricalFacts(false, List.of());

    var result = new RuleEvaluator(history).evaluate(rule, event);

    assertThat(result.status()).isEqualTo(RuleEvaluationStatus.NOT_EVALUATED);
  }

  @Test
  void anyShortCircuitsAfterAMatch() {
    var rule =
        new ExecutableRule(
            "rule-any",
            1,
            "HIGH",
            new AnyCondition(
                List.of(
                    new AmountThresholdCondition(10_000L, "BRL"),
                    new CountWindowCondition(600, 3))));

    var event =
        new TransactionEvent(
            1,
            "event-current",
            "transaction-current",
            "customer-001",
            15_000L,
            "BRL",
            Instant.parse("2026-09-06T12:00:00Z"),
            "PURCHASE",
            "ECOMMERCE",
            "BR",
            "device-hash",
            "trace-001");

    HistoricalFactsPort history =
        query -> {
          throw new AssertionError("History must not be queried after ANY already matched");
        };

    var result = new RuleEvaluator(history).evaluate(rule, event);

    assertThat(result.status()).isEqualTo(RuleEvaluationStatus.MATCHED);
  }

  @Test
  void allIsNotEvaluatedWhenAllKnownChildrenMatchAndOneCannotBeEvaluated() {
    var rule =
        new ExecutableRule(
            "rule-all",
            1,
            "HIGH",
            new AllCondition(
                List.of(
                    new AmountThresholdCondition(10_000L, "BRL"),
                    new CountWindowCondition(600, 3))));

    var occurredAt = Instant.parse("2026-09-06T12:00:00Z");

    var event =
        new TransactionEvent(
            1,
            "event-current",
            "transaction-current",
            "customer-001",
            15_000L,
            "BRL",
            occurredAt,
            "PURCHASE",
            "ECOMMERCE",
            "BR",
            "device-hash",
            "trace-001");

    HistoricalFactsPort history =
        query -> new HistoricalFactsPort.HistoricalFacts(false, List.of());

    var result = new RuleEvaluator(history).evaluate(rule, event);

    assertThat(result.status()).isEqualTo(RuleEvaluationStatus.NOT_EVALUATED);
  }

  @Test
  void allShortCircuitsAfterNoMatch() {
    var rule =
        new ExecutableRule(
            "rule-all",
            1,
            "HIGH",
            new AllCondition(
                List.of(
                    new AmountThresholdCondition(10_000L, "BRL"),
                    new CountWindowCondition(600, 3))));

    var event =
        new TransactionEvent(
            1,
            "event-current",
            "transaction-current",
            "customer-001",
            5_000L,
            "BRL",
            Instant.parse("2026-09-06T12:00:00Z"),
            "PURCHASE",
            "ECOMMERCE",
            "BR",
            "device-hash",
            "trace-001");

    HistoricalFactsPort history =
        query -> {
          throw new AssertionError("History must not be queried after ALL already failed");
        };

    var result = new RuleEvaluator(history).evaluate(rule, event);

    assertThat(result.status()).isEqualTo(RuleEvaluationStatus.NO_MATCH);
  }

  @Test
  void matchedAmountResultCarriesRuleIdentitySeverityAndSafeEvidenceCode() {
    var rule =
        new ExecutableRule("rule-amount", 7, "HIGH", new AmountThresholdCondition(10_000L, "BRL"));

    var event =
        new TransactionEvent(
            1,
            "event-001",
            "transaction-001",
            "customer-001",
            15_000L,
            "BRL",
            Instant.parse("2026-09-06T12:00:00Z"),
            "PURCHASE",
            "ECOMMERCE",
            "BR",
            "device-hash",
            "trace-001");

    HistoricalFactsPort history =
        query -> {
          throw new AssertionError("Amount threshold must not query historical state");
        };

    var result = new RuleEvaluator(history).evaluate(rule, event);

    assertThat(result.ruleId()).isEqualTo("rule-amount");
    assertThat(result.ruleVersion()).isEqualTo(7);
    assertThat(result.severity()).isEqualTo("HIGH");
    assertThat(result.status()).isEqualTo(RuleEvaluationStatus.MATCHED);
    assertThat(result.evidenceCodes()).containsExactly("AMOUNT_AT_OR_ABOVE_THRESHOLD");
  }

  @Test
  void matchedAmountResultCarriesOnlySanitizedEvaluatedValues() {
    var rule =
        new ExecutableRule("rule-amount", 7, "HIGH", new AmountThresholdCondition(10_000L, "BRL"));

    var event =
        new TransactionEvent(
            1,
            "event-001",
            "transaction-001",
            "customer-secret",
            15_000L,
            "BRL",
            Instant.parse("2026-09-06T12:00:00Z"),
            "PURCHASE",
            "ECOMMERCE",
            "BR",
            "device-secret",
            "trace-secret");

    HistoricalFactsPort history =
        query -> {
          throw new AssertionError("Amount threshold must not query historical state");
        };

    var result = new RuleEvaluator(history).evaluate(rule, event);

    assertThat(result.evaluatedValues())
        .containsExactlyInAnyOrderEntriesOf(
            Map.of(
                "amountMinor", 15_000L,
                "thresholdMinor", 10_000L,
                "currency", "BRL"));

    assertThat(result.evaluatedValues())
        .doesNotContainKeys(
            "customerId",
            "transactionId",
            "eventId",
            "deviceIdHash",
            "traceId",
            "ruleExpression",
            "rawPayload");
  }

  @Test
  void matchedCountWindowCarriesSanitizedEvidence() {
    var occurredAt = Instant.parse("2026-09-06T12:00:00Z");

    var rule = new ExecutableRule("rule-count", 2, "MEDIUM", new CountWindowCondition(600, 3));

    var event =
        transactionEvent(
            "event-current", "transaction-current", "customer-001", 1_000L, "BRL", occurredAt);

    HistoricalFactsPort history =
        query ->
            new HistoricalFactsPort.HistoricalFacts(
                true,
                List.of(
                    transactionEvent(
                        "event-previous-1",
                        "transaction-previous-1",
                        "customer-001",
                        500L,
                        "BRL",
                        occurredAt.minusSeconds(100)),
                    transactionEvent(
                        "event-previous-2",
                        "transaction-previous-2",
                        "customer-001",
                        700L,
                        "BRL",
                        occurredAt.minusSeconds(300))));

    var result = new RuleEvaluator(history).evaluate(rule, event);

    assertThat(result.status()).isEqualTo(RuleEvaluationStatus.MATCHED);
    assertThat(result.evidenceCodes()).containsExactly("COUNT_AT_OR_ABOVE_MINIMUM");
    assertThat(result.evaluatedValues())
        .containsExactlyInAnyOrderEntriesOf(
            Map.of(
                "observedCount", 3,
                "minimumCount", 3,
                "windowSeconds", 600));
  }

  @Test
  void unavailableHistoryProducesSanitizedNotEvaluatedEvidence() {
    var occurredAt = Instant.parse("2026-09-06T12:00:00Z");

    var rule = new ExecutableRule("rule-count", 2, "MEDIUM", new CountWindowCondition(600, 3));

    var event =
        transactionEvent(
            "event-current", "transaction-current", "customer-001", 1_000L, "BRL", occurredAt);

    HistoricalFactsPort history =
        query -> new HistoricalFactsPort.HistoricalFacts(false, List.of());

    var result = new RuleEvaluator(history).evaluate(rule, event);

    assertThat(result.status()).isEqualTo(RuleEvaluationStatus.NOT_EVALUATED);
    assertThat(result.evidenceCodes()).containsExactly("HISTORY_UNAVAILABLE");
    assertThat(result.evaluatedValues())
        .containsExactlyInAnyOrderEntriesOf(
            Map.of(
                "minimumCount", 3,
                "windowSeconds", 600));
  }

  @Test
  void amountBelowThresholdCarriesSafeNoMatchEvidence() {
    var rule =
        new ExecutableRule("rule-amount", 1, "HIGH", new AmountThresholdCondition(10_000L, "BRL"));

    var event =
        transactionEvent(
            "event-001",
            "transaction-001",
            "customer-001",
            9_999L,
            "BRL",
            Instant.parse("2026-09-06T12:00:00Z"));

    HistoricalFactsPort history =
        query -> {
          throw new AssertionError("Amount threshold must not query historical state");
        };

    var result = new RuleEvaluator(history).evaluate(rule, event);

    assertThat(result.status()).isEqualTo(RuleEvaluationStatus.NO_MATCH);
    assertThat(result.evidenceCodes()).containsExactly("AMOUNT_BELOW_THRESHOLD");
  }

  @Test
  void differentCurrencyDoesNotMatchAndExplainsWhy() {
    var rule =
        new ExecutableRule("rule-amount", 1, "HIGH", new AmountThresholdCondition(10_000L, "BRL"));

    var event =
        transactionEvent(
            "event-001",
            "transaction-001",
            "customer-001",
            15_000L,
            "USD",
            Instant.parse("2026-09-06T12:00:00Z"));

    HistoricalFactsPort history =
        query -> {
          throw new AssertionError("Amount threshold must not query historical state");
        };

    var result = new RuleEvaluator(history).evaluate(rule, event);

    assertThat(result.status()).isEqualTo(RuleEvaluationStatus.NO_MATCH);
    assertThat(result.evidenceCodes()).containsExactly("CURRENCY_MISMATCH");
    assertThat(result.evaluatedValues())
        .containsExactlyInAnyOrderEntriesOf(
            Map.of(
                "amountMinor",
                15_000L,
                "thresholdMinor",
                10_000L,
                "eventCurrency",
                "USD",
                "ruleCurrency",
                "BRL"));
  }

  private TransactionEvent transactionEvent(
      String eventId,
      String transactionId,
      String customerId,
      long amountMinor,
      String currency,
      Instant occurredAt) {

    return new TransactionEvent(
        1,
        eventId,
        transactionId,
        customerId,
        amountMinor,
        currency,
        occurredAt,
        "PURCHASE",
        "ECOMMERCE",
        "BR",
        "device-hash",
        "trace-id");
  }
}
