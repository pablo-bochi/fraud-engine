package com.fraudengine.control.observability;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

@Component
public final class RulesetMetrics {

  public RulesetMetrics(MeterRegistry registry, JdbcTemplate jdbc) {

    Gauge.builder("fraud.ruleset.desired.version", jdbc, RulesetMetrics::desiredVersion)
        .description("Desired ruleset version")
        .register(registry);

    Gauge.builder("fraud.ruleset.published.version", jdbc, RulesetMetrics::publishedVersion)
        .description("Published ruleset version")
        .register(registry);
  }

  private static double desiredVersion(JdbcTemplate jdbc) {

    Long value =
        jdbc.queryForObject(
            """
            SELECT desired_version
            FROM rules.ruleset_head
            WHERE head_key = 'ACTIVE'
            """,
            Long.class);

    return value == null ? 0 : value;
  }

  private static double publishedVersion(JdbcTemplate jdbc) {

    Long value =
        jdbc.queryForObject(
            """
            SELECT published_version
            FROM rules.ruleset_head
            WHERE head_key = 'ACTIVE'
            """,
            Long.class);

    return value == null ? 0 : value;
  }
}
