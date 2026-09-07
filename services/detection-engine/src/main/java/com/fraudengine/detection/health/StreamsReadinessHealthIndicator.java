package com.fraudengine.detection.health;

import java.util.concurrent.atomic.AtomicLong;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;

public final class StreamsReadinessHealthIndicator implements HealthIndicator {
  private final AtomicLong loadedVersion = new AtomicLong(-1);

  public void markRulesetLoaded(long version) {
    if (version > 0) loadedVersion.accumulateAndGet(version, Math::max);
  }

  @Override
  public Health health() {
    long version = loadedVersion.get();
    return version > 0
        ? Health.up().withDetail("loadedVersion", version).build()
        : Health.down().withDetail("reason", "NO_VALID_RULESET_LOADED").build();
  }

  public long loadedVersion() {
    return loadedVersion.get();
  }
}
