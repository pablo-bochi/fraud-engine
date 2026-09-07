package com.fraudengine.notification.adapter.out.persistence;

import static org.assertj.core.api.Assertions.assertThat;

import com.fraudengine.contracts.CustomerNotificationRequested;
import com.fraudengine.notification.application.port.NotificationDeliveryPort.ClaimResult;
import java.time.Instant;
import javax.sql.DataSource;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

@Testcontainers
class NotificationDeliveryPersistenceIntegrationTest {

  private static final Instant CLAIMED_AT = Instant.parse("2026-09-06T18:00:00Z");

  @Container
  static final PostgreSQLContainer<?> POSTGRES =
      new PostgreSQLContainer<>("postgres:17.11-alpine3.24");

  private JdbcTemplate jdbc;
  private JdbcNotificationDeliveryAdapter repository;

  @BeforeEach
  void setUp() {
    DataSource dataSource =
        new DriverManagerDataSource(
            POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword());

    Flyway.configure()
        .dataSource(dataSource)
        .schemas("notification")
        .defaultSchema("notification")
        .load()
        .migrate();

    jdbc = new JdbcTemplate(dataSource);

    jdbc.execute("TRUNCATE TABLE notification.delivery");

    repository = new JdbcNotificationDeliveryAdapter(jdbc);
  }

  @Test
  void firstClaimCreatesSingleDeliveryAndTransitionsItToSending() {
    ClaimResult result = repository.claim(request(), CLAIMED_AT);

    assertThat(result)
        .isInstanceOfSatisfying(
            ClaimResult.Acquired.class,
            acquired -> assertThat(acquired.attemptCount()).isEqualTo(1));

    assertThat(
            jdbc.queryForObject(
                """
                SELECT count(*)
                FROM notification.delivery
                WHERE notification_request_id = ?
                """,
                Integer.class,
                "notification-123"))
        .isEqualTo(1);

    assertThat(
            jdbc.queryForObject(
                """
                SELECT status
                FROM notification.delivery
                WHERE notification_request_id = ?
                """,
                String.class,
                "notification-123"))
        .isEqualTo("SENDING");

    assertThat(
            jdbc.queryForObject(
                """
                SELECT attempt_count
                FROM notification.delivery
                WHERE notification_request_id = ?
                """,
                Integer.class,
                "notification-123"))
        .isEqualTo(1);

    assertThat(
            jdbc.queryForObject(
                """
                SELECT trace_id
                FROM notification.delivery
                WHERE notification_request_id = ?
                """,
                String.class,
                "notification-123"))
        .isEqualTo("trace-123");
  }

  private static CustomerNotificationRequested request() {
    return new CustomerNotificationRequested(
        1,
        "notification-123",
        "alert-123",
        "customer-123",
        "transaction-123",
        "SUSPICIOUS_TRANSACTION",
        "suspicious-transaction-v1",
        "pt-BR",
        Instant.parse("2026-09-06T17:59:00Z"),
        "trace-123");
  }

  @Test
  void failedDeliveryCanBeRetriedAndSentDeliveryIsReused() {
    var request = request();

    var firstClaim = repository.claim(request, CLAIMED_AT);

    assertThat(firstClaim)
        .isInstanceOfSatisfying(
            ClaimResult.Acquired.class,
            acquired -> assertThat(acquired.attemptCount()).isEqualTo(1));

    Instant failedAt = CLAIMED_AT.plusSeconds(5);

    var failedResult =
        repository.markFailed(
            request,
            "EMAIL",
            1,
            "CHANNEL_DELIVERY_FAILED",
            failedAt);

    assertThat(failedResult.status()).isEqualTo("FAILED");
    assertThat(failedResult.attemptCount()).isEqualTo(1);
    assertThat(failedResult.channel()).isEqualTo("EMAIL");
    assertThat(failedResult.providerReference()).isNull();
    assertThat(failedResult.reasonCode()).isEqualTo("CHANNEL_DELIVERY_FAILED");
    assertThat(failedResult.updatedAt()).isEqualTo(failedAt);

    assertThat(
            jdbc.queryForObject(
                """
                SELECT status
                FROM notification.delivery
                WHERE notification_request_id = ?
                """,
                String.class,
                request.notificationRequestId()))
        .isEqualTo("FAILED");

    assertThat(
            jdbc.queryForObject(
                """
                SELECT last_error_code
                FROM notification.delivery
                WHERE notification_request_id = ?
                """,
                String.class,
                request.notificationRequestId()))
        .isEqualTo("CHANNEL_DELIVERY_FAILED");

    Instant retriedAt = CLAIMED_AT.plusSeconds(10);

    var retryClaim = repository.claim(request, retriedAt);

    assertThat(retryClaim)
        .isInstanceOfSatisfying(
            ClaimResult.Acquired.class,
            acquired -> assertThat(acquired.attemptCount()).isEqualTo(2));

    assertThat(
            jdbc.queryForObject(
                """
                SELECT status
                FROM notification.delivery
                WHERE notification_request_id = ?
                """,
                String.class,
                request.notificationRequestId()))
        .isEqualTo("SENDING");

    assertThat(
            jdbc.queryForObject(
                """
                SELECT attempt_count
                FROM notification.delivery
                WHERE notification_request_id = ?
                """,
                Integer.class,
                request.notificationRequestId()))
        .isEqualTo(2);

    assertThat(
            jdbc.queryForObject(
                """
                SELECT last_error_code
                FROM notification.delivery
                WHERE notification_request_id = ?
                """,
                String.class,
                request.notificationRequestId()))
        .isNull();

    Instant sentAt = CLAIMED_AT.plusSeconds(15);

    var sentResult =
        repository.markSent(
            request,
            "EMAIL",
            "mailpit-message-123",
            2,
            sentAt);

    assertThat(sentResult.status()).isEqualTo("SENT");
    assertThat(sentResult.attemptCount()).isEqualTo(2);
    assertThat(sentResult.channel()).isEqualTo("EMAIL");
    assertThat(sentResult.providerReference()).isEqualTo("mailpit-message-123");
    assertThat(sentResult.reasonCode()).isNull();
    assertThat(sentResult.updatedAt()).isEqualTo(sentAt);

    Instant replayedAt = CLAIMED_AT.plusSeconds(20);

    var replayClaim = repository.claim(request, replayedAt);

    assertThat(replayClaim)
        .isInstanceOfSatisfying(
            ClaimResult.AlreadySent.class,
            alreadySent -> assertThat(alreadySent.result()).isEqualTo(sentResult));

    assertThat(
            jdbc.queryForObject(
                """
                SELECT count(*)
                FROM notification.delivery
                WHERE notification_request_id = ?
                """,
                Integer.class,
                request.notificationRequestId()))
        .isEqualTo(1);

    assertThat(
            jdbc.queryForObject(
                """
                SELECT attempt_count
                FROM notification.delivery
                WHERE notification_request_id = ?
                """,
                Integer.class,
                request.notificationRequestId()))
        .isEqualTo(2);
  }

  @Test
  void concurrentClaimsAllowOnlyOneSendingWinner() throws Exception {
    var start = new CountDownLatch(1);

    try (var executor = Executors.newFixedThreadPool(2)) {
      var first =
          executor.submit(
              () -> {
                start.await();
                return repository.claim(request(), CLAIMED_AT);
              });

      var second =
          executor.submit(
              () -> {
                start.await();
                return repository.claim(request(), CLAIMED_AT);
              });

      start.countDown();

      List<ClaimResult> results =
          List.of(
              first.get(10, TimeUnit.SECONDS),
              second.get(10, TimeUnit.SECONDS));

      assertThat(results)
          .filteredOn(result -> result instanceof ClaimResult.Acquired)
          .singleElement()
          .isInstanceOfSatisfying(
              ClaimResult.Acquired.class,
              acquired -> assertThat(acquired.attemptCount()).isEqualTo(1));

      assertThat(results)
          .filteredOn(result -> result instanceof ClaimResult.InProgress)
          .hasSize(1);

      assertThat(
              jdbc.queryForObject(
                  """
                  SELECT count(*)
                  FROM notification.delivery
                  WHERE notification_request_id = ?
                  """,
                  Integer.class,
                  "notification-123"))
          .isEqualTo(1);

      assertThat(
              jdbc.queryForObject(
                  """
                  SELECT status
                  FROM notification.delivery
                  WHERE notification_request_id = ?
                  """,
                  String.class,
                  "notification-123"))
          .isEqualTo("SENDING");

      assertThat(
              jdbc.queryForObject(
                  """
                  SELECT attempt_count
                  FROM notification.delivery
                  WHERE notification_request_id = ?
                  """,
                  Integer.class,
                  "notification-123"))
          .isEqualTo(1);
    }
  }
}
