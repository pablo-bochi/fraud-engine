package com.fraudengine.detection.adapter.in.kafka;

import com.fraudengine.contracts.TransactionEvent;
import java.util.List;

public record CustomerHistory(List<TransactionEvent> events) {

  public CustomerHistory {
    events = List.copyOf(events);
  }
}
