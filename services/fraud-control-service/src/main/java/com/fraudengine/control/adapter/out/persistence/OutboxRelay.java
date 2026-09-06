package com.fraudengine.control.adapter.out.persistence;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

public final class OutboxRelay {
  private static final Logger LOGGER = LoggerFactory.getLogger(OutboxRelay.class);
  private static final long MAXIMUM_BACKOFF_SECONDS = 60;
  private static final int FAILURE_ALERT_THRESHOLD = 5;
  private final JdbcTemplate jdbc;
  private final SnapshotPublisher publisher;
  private final TransactionTemplate transactions;

  public OutboxRelay(JdbcTemplate jdbc, SnapshotPublisher publisher) {
    this.jdbc = jdbc;
    this.publisher = publisher;
    this.transactions =
        new TransactionTemplate(new DataSourceTransactionManager(jdbc.getDataSource()));
  }

  public boolean relayNext() {
    UUID[] attemptedEvent = new UUID[1];
    try {
      Boolean published =
          transactions.execute(
              status -> {
                jdbc.queryForObject(
                    "SELECT head_key FROM rules.ruleset_head WHERE head_key = 'ACTIVE' FOR UPDATE",
                    String.class);
                PendingOutbox event =
                    jdbc
                        .query(
                            """
                          SELECT outbox_event_id, aggregate_version, canonical_payload, next_attempt_at
                          FROM rules.outbox_event
                          WHERE status = 'PENDING'
                          ORDER BY aggregate_version
                          LIMIT 1 FOR UPDATE
                          """,
                            (resultSet, rowNum) ->
                                new PendingOutbox(
                                    resultSet.getObject("outbox_event_id", UUID.class),
                                    resultSet.getLong("aggregate_version"),
                                    resultSet.getString("canonical_payload"),
                                    resultSet.getTimestamp("next_attempt_at").toInstant()))
                        .stream()
                        .findFirst()
                        .orElse(null);
                if (event == null || event.nextAttemptAt().isAfter(Instant.now())) {
                  return false;
                }
                attemptedEvent[0] = event.outboxEventId();
                RulesetSnapshotVerifier.verify(event.canonicalPayload(), event.aggregateVersion());
                publisher.publish("ACTIVE", event.canonicalPayload());
                Instant now = Instant.now();
                jdbc.update(
                    """
                  UPDATE rules.outbox_event
                  SET status = 'PUBLISHED', published_at = ?
                  WHERE outbox_event_id = ? AND status = 'PENDING'
                  """,
                    Timestamp.from(now),
                    event.outboxEventId());
                jdbc.update(
                    "UPDATE rules.ruleset_head SET published_version = ? WHERE head_key = 'ACTIVE'",
                    event.aggregateVersion());
                return true;
              });
      return Boolean.TRUE.equals(published);
    } catch (RuntimeException error) {
      if (attemptedEvent[0] != null) {
        recordFailure(attemptedEvent[0]);
        return false;
      }
      throw error;
    }
  }

  private void recordFailure(UUID outboxEventId) {
    transactions.executeWithoutResult(
        status -> {
          jdbc.queryForObject(
              "SELECT head_key FROM rules.ruleset_head WHERE head_key = 'ACTIVE' FOR UPDATE",
              String.class);
          int previousAttempts =
              jdbc.queryForObject(
                  "SELECT attempt_count FROM rules.outbox_event WHERE outbox_event_id = ? FOR UPDATE",
                  Integer.class,
                  outboxEventId);
          int nextAttempts = previousAttempts + 1;
          long delaySeconds =
              Math.min(MAXIMUM_BACKOFF_SECONDS, 1L << Math.min(previousAttempts, 6));
          jdbc.update(
              """
              UPDATE rules.outbox_event
              SET attempt_count = attempt_count + 1,
                next_attempt_at = ?, last_error_code = 'PUBLISH_FAILED'
              WHERE outbox_event_id = ? AND status = 'PENDING'
              """,
              Timestamp.from(Instant.now().plusSeconds(delaySeconds)),
              outboxEventId);
          if (nextAttempts >= FAILURE_ALERT_THRESHOLD) {
            LOGGER.error(
                "outbox publication repeatedly failed: eventId={}, attempts={}",
                outboxEventId,
                nextAttempts);
          }
        });
  }

  private record PendingOutbox(
      UUID outboxEventId, long aggregateVersion, String canonicalPayload, Instant nextAttemptAt) {}
}
