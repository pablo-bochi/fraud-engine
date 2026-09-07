package com.fraudengine.detection.health;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class StreamsReadinessHealthIndicatorTest {
  @Test
  void isDownUntilAValidRulesetHasBeenLoaded() {
    StreamsReadinessHealthIndicator indicator = new StreamsReadinessHealthIndicator();
    assertThat(indicator.health().getStatus().getCode()).isEqualTo("DOWN");
    indicator.markRulesetLoaded(4L);
    assertThat(indicator.health().getStatus().getCode()).isEqualTo("UP");
    assertThat(indicator.health().getDetails()).containsEntry("loadedVersion", 4L);
  }
}
