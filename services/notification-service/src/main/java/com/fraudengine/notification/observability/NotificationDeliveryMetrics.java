package com.fraudengine.notification.observability;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

@Component
public final class NotificationDeliveryMetrics {

  public NotificationDeliveryMetrics(MeterRegistry registry, JdbcTemplate jdbc) {

    register(registry, jdbc, "SENT");
    register(registry, jdbc, "FAILED");
  }

  private static void register(MeterRegistry registry, JdbcTemplate jdbc, String status) {

    Gauge.builder("fraud.notification.deliveries", jdbc, source -> count(source, status))
        .description("Current persisted notification deliveries")
        .tag("status", status)
        .register(registry);
  }

  private static double count(JdbcTemplate jdbc, String status) {

    Long value =
        jdbc.queryForObject(
            """
            SELECT count(*)
            FROM notification.delivery
            WHERE status = ?
            """,
            Long.class,
            status);

    return value == null ? 0 : value;
  }
}
