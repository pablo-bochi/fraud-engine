package com.fraudengine.detection.domain.rules;

import com.fraudengine.contracts.TransactionEvent;
import java.time.Instant;
import java.util.List;

@FunctionalInterface
public interface HistoricalFactsPort {

  HistoricalFacts load(Query query);

  record Query(String customerId, Instant from, Instant until) {}

  record HistoricalFacts(boolean available, List<TransactionEvent> events) {}
}
