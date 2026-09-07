package com.fraudengine.notification.observability;

import static org.assertj.core.api.Assertions.assertThat;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.core.env.MapPropertySource;
import org.springframework.jdbc.core.JdbcTemplate;

class NotificationDeliveryMetricsTest {

  @Test
  void registersDeliveryGaugesWhenJdbcIsAvailable() {
    try (AnnotationConfigApplicationContext context = contextWithMetricsDependencies(true, true)) {
      assertThat(context.getBeansOfType(NotificationDeliveryMetrics.class)).hasSize(1);

      MeterRegistry registry = context.getBean(MeterRegistry.class);
      assertThat(registry.find("fraud.notification.deliveries").tag("status", "SENT").gauge())
          .isNotNull();
      assertThat(registry.find("fraud.notification.deliveries").tag("status", "FAILED").gauge())
          .isNotNull();
    }
  }

  @Test
  void skipsDeliveryGaugesWhenPersistenceIsDisabled() {
    try (AnnotationConfigApplicationContext context =
        contextWithMetricsDependencies(false, false)) {
      assertThat(context.getBeansOfType(NotificationDeliveryMetrics.class)).isEmpty();
      assertThat(
              context.getBean(MeterRegistry.class).find("fraud.notification.deliveries").meters())
          .isEmpty();
    }
  }

  private static AnnotationConfigApplicationContext contextWithMetricsDependencies(
      boolean persistenceEnabled, boolean jdbcAvailable) {
    AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext();
    context
        .getEnvironment()
        .getPropertySources()
        .addFirst(
            new MapPropertySource(
                "test", Map.of("notification.persistence.enabled", persistenceEnabled)));
    context.registerBean(MeterRegistry.class, SimpleMeterRegistry::new);
    if (jdbcAvailable) {
      context.registerBean(
          JdbcTemplate.class,
          () ->
              new JdbcTemplate() {
                @Override
                public void afterPropertiesSet() {
                  // Gauge registration does not query the database until collection time.
                }
              });
    }
    context.register(NotificationDeliveryMetrics.class);
    context.refresh();
    return context;
  }
}
