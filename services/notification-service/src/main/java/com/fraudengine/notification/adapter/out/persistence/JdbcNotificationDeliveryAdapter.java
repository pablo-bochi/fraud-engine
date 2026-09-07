package com.fraudengine.notification.adapter.out.persistence;

import com.fraudengine.contracts.CustomerNotificationRequested;
import com.fraudengine.contracts.NotificationResult;
import com.fraudengine.notification.application.port.NotificationDeliveryPort;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

public final class JdbcNotificationDeliveryAdapter implements NotificationDeliveryPort {

  private final JdbcTemplate jdbc;
  private final TransactionTemplate transactions;

  public JdbcNotificationDeliveryAdapter(JdbcTemplate jdbc) {
    this.jdbc = Objects.requireNonNull(jdbc);

    var dataSource =
        Objects.requireNonNull(jdbc.getDataSource(), "JdbcTemplate must have a DataSource");

    this.transactions = new TransactionTemplate(new DataSourceTransactionManager(dataSource));
  }

  @Override
  public ClaimResult claim(CustomerNotificationRequested request, Instant claimedAt) {
    ClaimResult result =
        transactions.execute(
            ignored -> {
              insertIfAbsent(request, claimedAt);

              List<Integer> acquiredAttempts =
                  jdbc.query(
                      """
                      UPDATE notification.delivery
                      SET
                        status = 'SENDING',
                        attempt_count = attempt_count + 1,
                        provider_reference = NULL,
                        last_error_code = NULL,
                        updated_at = ?
                      WHERE notification_request_id = ?
                        AND status IN ('PENDING', 'FAILED')
                      RETURNING attempt_count
                      """,
                      (resultSet, rowNum) -> resultSet.getInt("attempt_count"),
                      Timestamp.from(claimedAt),
                      request.notificationRequestId());

              if (!acquiredAttempts.isEmpty()) {
                return new ClaimResult.Acquired(acquiredAttempts.getFirst());
              }

              return loadExistingClaimResult(request);
            });

    return Objects.requireNonNull(result);
  }

  @Override
  public NotificationResult markSent(
      CustomerNotificationRequested request,
      String channel,
      String providerReference,
      int attemptCount,
      Instant sentAt) {

    NotificationResult result =
        transactions.execute(
            ignored -> {
              List<NotificationResult> updated =
                  jdbc.query(
                      """
                      UPDATE notification.delivery
                      SET
                        status = 'SENT',
                        channel = ?,
                        provider_reference = ?,
                        last_error_code = NULL,
                        updated_at = ?
                      WHERE notification_request_id = ?
                        AND status = 'SENDING'
                        AND attempt_count = ?
                      RETURNING
                        notification_request_id,
                        channel,
                        status,
                        attempt_count,
                        provider_reference,
                        last_error_code,
                        updated_at,
                        trace_id
                      """,
                      (resultSet, rowNum) -> notificationResult(request.schemaVersion(), resultSet),
                      channel,
                      providerReference,
                      Timestamp.from(sentAt),
                      request.notificationRequestId(),
                      attemptCount);

              if (updated.size() != 1) {
                throw new IllegalStateException("DELIVERY_NOT_SENDING");
              }

              return updated.getFirst();
            });

    return Objects.requireNonNull(result);
  }

  @Override
  public NotificationResult markFailed(
      CustomerNotificationRequested request,
      String channel,
      int attemptCount,
      String reasonCode,
      Instant failedAt) {

    NotificationResult result =
        transactions.execute(
            ignored -> {
              List<NotificationResult> updated =
                  jdbc.query(
                      """
                      UPDATE notification.delivery
                      SET
                        status = 'FAILED',
                        channel = ?,
                        provider_reference = NULL,
                        last_error_code = ?,
                        updated_at = ?
                      WHERE notification_request_id = ?
                        AND status = 'SENDING'
                        AND attempt_count = ?
                      RETURNING
                        notification_request_id,
                        channel,
                        status,
                        attempt_count,
                        provider_reference,
                        last_error_code,
                        updated_at,
                        trace_id
                      """,
                      (resultSet, rowNum) -> notificationResult(request.schemaVersion(), resultSet),
                      channel,
                      reasonCode,
                      Timestamp.from(failedAt),
                      request.notificationRequestId(),
                      attemptCount);

              if (updated.size() != 1) {
                throw new IllegalStateException("DELIVERY_NOT_SENDING");
              }

              return updated.getFirst();
            });

    return Objects.requireNonNull(result);
  }

  private void insertIfAbsent(CustomerNotificationRequested request, Instant createdAt) {
    jdbc.update(
        """
        INSERT INTO notification.delivery(
          notification_request_id,
          alert_id,
          customer_id,
          transaction_id,
          category,
          template_id,
          locale,
          trace_id,
          requested_at,
          status,
          attempt_count,
          created_at,
          updated_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'PENDING', 0, ?, ?)
        ON CONFLICT (notification_request_id) DO NOTHING
        """,
        request.notificationRequestId(),
        request.alertId(),
        request.customerId(),
        request.transactionId(),
        request.category(),
        request.templateId(),
        request.locale(),
        request.traceId(),
        Timestamp.from(request.requestedAt()),
        Timestamp.from(createdAt),
        Timestamp.from(createdAt));
  }

  private ClaimResult loadExistingClaimResult(CustomerNotificationRequested request) {
    StoredDelivery delivery =
        jdbc.queryForObject(
            """
            SELECT
              notification_request_id,
              channel,
              status,
              attempt_count,
              provider_reference,
              last_error_code,
              updated_at,
              trace_id
            FROM notification.delivery
            WHERE notification_request_id = ?
            """,
            (resultSet, rowNum) ->
                new StoredDelivery(
                    resultSet.getString("notification_request_id"),
                    resultSet.getString("channel"),
                    resultSet.getString("status"),
                    resultSet.getInt("attempt_count"),
                    resultSet.getString("provider_reference"),
                    resultSet.getString("last_error_code"),
                    resultSet.getTimestamp("updated_at").toInstant(),
                    resultSet.getString("trace_id")),
            request.notificationRequestId());

    if ("SENT".equals(delivery.status())) {
      return new ClaimResult.AlreadySent(
          new NotificationResult(
              request.schemaVersion(),
              delivery.notificationRequestId(),
              delivery.channel(),
              delivery.status(),
              delivery.attemptCount(),
              delivery.providerReference(),
              delivery.lastErrorCode(),
              delivery.updatedAt(),
              delivery.traceId()));
    }

    if ("SENDING".equals(delivery.status())) {
      return new ClaimResult.InProgress(delivery.attemptCount());
    }

    throw new IllegalStateException("UNEXPECTED_DELIVERY_STATUS: " + delivery.status());
  }

  private NotificationResult notificationResult(int schemaVersion, java.sql.ResultSet resultSet)
      throws java.sql.SQLException {
    return new NotificationResult(
        schemaVersion,
        resultSet.getString("notification_request_id"),
        resultSet.getString("channel"),
        resultSet.getString("status"),
        resultSet.getInt("attempt_count"),
        resultSet.getString("provider_reference"),
        resultSet.getString("last_error_code"),
        resultSet.getTimestamp("updated_at").toInstant(),
        resultSet.getString("trace_id"));
  }

  private record StoredDelivery(
      String notificationRequestId,
      String channel,
      String status,
      int attemptCount,
      String providerReference,
      String lastErrorCode,
      Instant updatedAt,
      String traceId) {}
}
