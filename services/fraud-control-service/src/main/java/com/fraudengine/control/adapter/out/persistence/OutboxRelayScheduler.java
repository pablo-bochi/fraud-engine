package com.fraudengine.control.adapter.out.persistence;

import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
public class OutboxRelayScheduler {
  private final OutboxRelay relay;

  public OutboxRelayScheduler(OutboxRelay relay) {
    this.relay = relay;
  }

  @Scheduled(fixedDelayString = "${control.outbox.fixed-delay-ms:1000}")
  public void publishPendingSnapshots() {
    relay.relayNext();
  }
}
