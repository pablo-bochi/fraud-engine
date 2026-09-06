package com.fraudengine.control.adapter.out.persistence;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fraudengine.control.application.ApprovalResult;
import com.fraudengine.control.application.RuleProposal;
import com.fraudengine.control.domain.RuleDefinitionValidator;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import javax.sql.DataSource;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers
class RulePersistenceIntegrationTest {
  @Container
  static final PostgreSQLContainer<?> POSTGRES =
      new PostgreSQLContainer<>("postgres:17.11-alpine3.24");

  private JdbcTemplate jdbc;
  private JdbcRuleRepository repository;

  @BeforeEach
  void setUp() {
    DataSource dataSource =
        new DriverManagerDataSource(
            POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword());
    Flyway.configure()
        .dataSource(dataSource)
        .schemas("rules")
        .defaultSchema("rules")
        .load()
        .migrate();
    jdbc = new JdbcTemplate(dataSource);
    jdbc.execute(
        "TRUNCATE TABLE rules.audit_event, rules.outbox_event, rules.rule_version, rules.rule CASCADE");
    jdbc.execute(
        "INSERT INTO rules.ruleset_head(head_key) VALUES ('ACTIVE') ON CONFLICT DO NOTHING");
    repository =
        new JdbcRuleRepository(jdbc, new RuleDefinitionValidator(8, 900), new ObjectMapper());
  }

  @Test
  void createsRulePendingVersionAndCreationAuditAtomically() throws Exception {
    RuleProposal proposal =
        repository.createProposal(
            "high-amount",
            "High amount",
            new ObjectMapper()
                .readTree(
                    """
        {"type":"AMOUNT_THRESHOLD","amountMinor":10000,"currency":"BRL"}
        """),
            "rule-author");
    UUID ruleId = proposal.ruleId();

    assertThat(
            jdbc.queryForObject(
                "SELECT count(*) FROM rules.rule WHERE rule_id = ?", Integer.class, ruleId))
        .isEqualTo(1);
    assertThat(
            jdbc.queryForObject(
                "SELECT status FROM rules.rule_version WHERE rule_id = ?", String.class, ruleId))
        .isEqualTo("PENDING_APPROVAL");
    String versionId =
        jdbc.queryForObject(
            "SELECT rule_version_id FROM rules.rule_version WHERE rule_id = ?",
            String.class,
            ruleId);
    assertThat(
            jdbc.queryForObject(
                "SELECT actor_subject FROM rules.audit_event WHERE entity_id = ?",
                String.class,
                versionId))
        .isEqualTo("rule-author");
  }

  @Test
  void exposesRulesVersionsAuditAndRulesetPublicationStatusForAuthorizedReads() throws Exception {
    var proposal =
        repository.createProposal("high-amount", "High amount", definition(), "rule-author");

    assertThat(repository.rules())
        .singleElement()
        .extracting(rule -> rule.ruleId())
        .isEqualTo(proposal.ruleId());
    assertThat(repository.versions(proposal.ruleId()))
        .singleElement()
        .satisfies(version -> assertThat(version.status()).isEqualTo("PENDING_APPROVAL"));
    assertThat(repository.auditEvents())
        .anySatisfy(event -> assertThat(event.actorSubject()).isEqualTo("rule-author"));
    assertThat(repository.activeRulesetStatus())
        .returns(0L, status -> status.desiredVersion())
        .returns(0L, status -> status.publishedVersion());
  }

  @Test
  void databasePreventsAnotherPendingVersionForTheSameRule() throws Exception {
    UUID ruleId =
        repository
            .createProposal("high-amount", "High amount", definition(), "rule-author")
            .ruleId();

    assertThatThrownBy(
            () -> repository.proposeChange(ruleId, "UPSERT", definition(), "rule-author"))
        .isInstanceOf(DataIntegrityViolationException.class);
  }

  @Test
  void aDecidedVersionAllowsTheNextMonotonicVersionForTheSameRule() throws Exception {
    var proposal =
        repository.createProposal("high-amount", "High amount", definition(), "rule-author");
    repository.reject(proposal.ruleVersionId(), "rule-approver");

    UUID nextVersion =
        repository.proposeChange(proposal.ruleId(), "UPSERT", definition(), "rule-author");

    assertThat(
            jdbc.queryForObject(
                "SELECT version_number FROM rules.rule_version WHERE rule_version_id = ?",
                Integer.class,
                nextVersion))
        .isEqualTo(2);
  }

  @Test
  void approvalCreatesTheDesiredSnapshotAndOutboxInOneTransaction() throws Exception {
    UUID ruleId =
        repository
            .createProposal("high-amount", "High amount", definition(), "rule-author")
            .ruleId();
    UUID versionId =
        jdbc.queryForObject(
            "SELECT rule_version_id FROM rules.rule_version WHERE rule_id = ?", UUID.class, ruleId);

    ApprovalResult result = repository.approve(versionId, "rule-approver");

    assertThat(result.desiredVersion()).isEqualTo(1);
    assertThat(result.publicationStatus()).isEqualTo("PENDING");
    assertThat(
            jdbc.queryForObject(
                "SELECT status FROM rules.rule_version WHERE rule_version_id = ?",
                String.class,
                versionId))
        .isEqualTo("APPROVED");
    assertThat(
            jdbc.queryForObject(
                "SELECT desired_version FROM rules.ruleset_head WHERE head_key = 'ACTIVE'",
                Long.class))
        .isEqualTo(1L);
    assertThat(
            jdbc.queryForObject(
                "SELECT count(*) FROM rules.ruleset_snapshot_item WHERE snapshot_id = ?",
                Integer.class,
                result.snapshotId()))
        .isEqualTo(1);
    assertThat(
            jdbc.queryForObject(
                "SELECT status FROM rules.outbox_event WHERE aggregate_version = 1", String.class))
        .isEqualTo("PENDING");
  }

  @Test
  void selfApprovalIsDeniedAndAuditedWithoutPromotingTheProposal() throws Exception {
    UUID ruleId =
        repository
            .createProposal("high-amount", "High amount", definition(), "rule-author")
            .ruleId();
    UUID versionId =
        jdbc.queryForObject(
            "SELECT rule_version_id FROM rules.rule_version WHERE rule_id = ?", UUID.class, ruleId);

    ApprovalResult result = repository.approve(versionId, "rule-author");

    assertThat(result.approved()).isFalse();
    assertThat(result.denialReason()).isEqualTo("SELF_APPROVAL");
    assertThat(
            jdbc.queryForObject(
                "SELECT status FROM rules.rule_version WHERE rule_version_id = ?",
                String.class,
                versionId))
        .isEqualTo("PENDING_APPROVAL");
    assertThat(jdbc.queryForObject("SELECT count(*) FROM rules.outbox_event", Integer.class))
        .isZero();
    assertThat(
            jdbc.queryForObject(
                "SELECT outcome FROM rules.audit_event WHERE entity_id = ? ORDER BY occurred_at DESC LIMIT 1",
                String.class,
                versionId.toString()))
        .isEqualTo("DENIED");
  }

  @Test
  void selfRejectionIsDeniedAndAuditedWithoutChangingTheProposal() throws Exception {
    UUID ruleId =
        repository
            .createProposal("high-amount", "High amount", definition(), "rule-author")
            .ruleId();
    UUID versionId =
        jdbc.queryForObject(
            "SELECT rule_version_id FROM rules.rule_version WHERE rule_id = ?", UUID.class, ruleId);

    assertThatThrownBy(() -> repository.reject(versionId, "rule-author"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("SELF_REJECTION");

    assertThat(
            jdbc.queryForObject(
                "SELECT status FROM rules.rule_version WHERE rule_version_id = ?",
                String.class,
                versionId))
        .isEqualTo("PENDING_APPROVAL");
    assertThat(
            jdbc.queryForObject(
                "SELECT outcome FROM rules.audit_event WHERE entity_id = ? ORDER BY occurred_at DESC LIMIT 1",
                String.class,
                versionId.toString()))
        .isEqualTo("DENIED");
  }

  @Test
  void retirementOfTheLastActiveRuleIsDeniedWithoutChangingTheCurrentSnapshot() throws Exception {
    UUID ruleId =
        repository
            .createProposal("high-amount", "High amount", definition(), "rule-author")
            .ruleId();
    UUID firstVersion =
        jdbc.queryForObject(
            "SELECT rule_version_id FROM rules.rule_version WHERE rule_id = ?", UUID.class, ruleId);
    ApprovalResult firstApproval = repository.approve(firstVersion, "rule-approver");
    UUID retirement = repository.proposeChange(ruleId, "RETIRE", null, "rule-author");

    ApprovalResult result = repository.approve(retirement, "rule-approver");

    assertThat(result.approved()).isFalse();
    assertThat(result.denialReason()).isEqualTo("ACTIVE_RULESET_MUST_NOT_BE_EMPTY");
    assertThat(
            jdbc.queryForObject(
                "SELECT desired_snapshot_id FROM rules.ruleset_head WHERE head_key = 'ACTIVE'",
                UUID.class))
        .isEqualTo(firstApproval.snapshotId());
    assertThat(
            jdbc.queryForObject(
                "SELECT status FROM rules.rule_version WHERE rule_version_id = ?",
                String.class,
                retirement))
        .isEqualTo("PENDING_APPROVAL");
    assertThat(jdbc.queryForObject("SELECT count(*) FROM rules.outbox_event", Integer.class))
        .isEqualTo(1);
  }

  @Test
  void approvedRetirementRemovesOnlyItsRuleAndKeepsTheSnapshotProvenance() throws Exception {
    var retained = repository.createProposal("retained", "Retained", definition(), "author-a");
    repository.approve(retained.ruleVersionId(), "approver-a");
    var retired = repository.createProposal("retired", "Retired", definition(), "author-b");
    repository.approve(retired.ruleVersionId(), "approver-b");
    UUID retirement = repository.proposeChange(retired.ruleId(), "RETIRE", null, "author-b");

    ApprovalResult result = repository.approve(retirement, "approver-c");

    assertThat(
            jdbc.query(
                """
                SELECT rule_id FROM rules.ruleset_snapshot_item item
                JOIN rules.rule_version version ON version.rule_version_id = item.rule_version_id
                WHERE item.snapshot_id = ?
                """,
                (resultSet, rowNum) -> resultSet.getObject("rule_id", UUID.class),
                result.snapshotId()))
        .containsExactly(retained.ruleId());
    assertThat(
            jdbc.queryForObject(
                """
                SELECT approved_change_rule_version_id FROM rules.ruleset_snapshot
                WHERE snapshot_id = ?
                """,
                UUID.class,
                result.snapshotId()))
        .isEqualTo(retirement);
  }

  @Test
  void approvalRollsBackEveryBusinessWriteWhenOutboxPersistenceFails() throws Exception {
    var proposal =
        repository.createProposal("high-amount", "High amount", definition(), "rule-author");
    jdbc.update(
        """
        INSERT INTO rules.outbox_event(
          outbox_event_id, aggregate_type, aggregate_id, aggregate_version, event_type,
          canonical_payload, status, next_attempt_at, created_at)
        VALUES (?, 'RULESET', 'ACTIVE', 1, 'TEST', '{}', 'PENDING', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
        """,
        UUID.randomUUID());

    assertThatThrownBy(() -> repository.approve(proposal.ruleVersionId(), "rule-approver"))
        .isInstanceOf(DataIntegrityViolationException.class);

    assertThat(
            jdbc.queryForObject(
                "SELECT status FROM rules.rule_version WHERE rule_version_id = ?",
                String.class,
                proposal.ruleVersionId()))
        .isEqualTo("PENDING_APPROVAL");
    assertThat(repository.activeRulesetStatus().desiredVersion()).isZero();
    assertThat(jdbc.queryForObject("SELECT count(*) FROM rules.ruleset_snapshot", Integer.class))
        .isZero();
    assertThat(
            jdbc.queryForObject(
                "SELECT count(*) FROM rules.audit_event WHERE action = 'RULE_VERSION_APPROVED'",
                Integer.class))
        .isZero();
  }

  @Test
  void approvalRejectsACandidateWithMoreThanOneHundredRulesBeforeChangingState() throws Exception {
    UUID snapshotId = UUID.randomUUID();
    List<UUID> activeVersions = new ArrayList<>();
    for (int index = 0; index < 100; index++) {
      UUID ruleId = UUID.randomUUID();
      UUID versionId = UUID.randomUUID();
      activeVersions.add(versionId);
      jdbc.update(
          "INSERT INTO rules.rule(rule_id, rule_key, name, created_by_subject, created_at) VALUES (?, ?, ?, 'seed', CURRENT_TIMESTAMP)",
          ruleId,
          "seed-" + index,
          "Seed " + index);
      jdbc.update(
          """
          INSERT INTO rules.rule_version(
            rule_version_id, rule_id, version_number, change_type, status, definition, author_subject)
          VALUES (?, ?, 1, 'UPSERT', 'APPROVED', CAST(? AS jsonb), 'seed')
          """,
          versionId,
          ruleId,
          definition().toString());
    }
    jdbc.update(
        """
        INSERT INTO rules.ruleset_snapshot(
          snapshot_id, approved_change_rule_version_id, version, content_hash, created_at)
        VALUES (?, ?, 1, 'sha256:seed', CURRENT_TIMESTAMP)
        """,
        snapshotId,
        activeVersions.getFirst());
    for (int index = 0; index < activeVersions.size(); index++) {
      jdbc.update(
          """
          INSERT INTO rules.ruleset_snapshot_item(snapshot_id, rule_version_id, evaluation_order)
          VALUES (?, ?, ?)
          """,
          snapshotId,
          activeVersions.get(index),
          index);
    }
    jdbc.update(
        "UPDATE rules.ruleset_head SET desired_snapshot_id = ?, desired_version = 1 WHERE head_key = 'ACTIVE'",
        snapshotId);
    var proposal =
        repository.createProposal("over-limit", "Over limit", definition(), "rule-author");

    assertThatThrownBy(() -> repository.approve(proposal.ruleVersionId(), "rule-approver"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("RULESET_SIZE_EXCEEDS_LIMIT");
    assertThat(
            jdbc.queryForObject(
                "SELECT status FROM rules.rule_version WHERE rule_version_id = ?",
                String.class,
                proposal.ruleVersionId()))
        .isEqualTo("PENDING_APPROVAL");
    assertThat(jdbc.queryForObject("SELECT count(*) FROM rules.outbox_event", Integer.class))
        .isZero();
  }

  @Test
  void approvalRejectsAPayloadLargerThanThePublicationLimitBeforeChangingState() throws Exception {
    var largeDefinition =
        new ObjectMapper()
            .readTree(
                """
                {"type":"AMOUNT_THRESHOLD","amountMinor":1,"currency":"BRL","padding":"%s"}
                """
                    .formatted("x".repeat(1_048_576)));
    var proposal =
        repository.createProposal("large-payload", "Large payload", largeDefinition, "rule-author");

    assertThatThrownBy(() -> repository.approve(proposal.ruleVersionId(), "rule-approver"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("RULESET_PAYLOAD_EXCEEDS_LIMIT");
    assertThat(repository.activeRulesetStatus().desiredVersion()).isZero();
    assertThat(
            jdbc.queryForObject(
                "SELECT status FROM rules.rule_version WHERE rule_version_id = ?",
                String.class,
                proposal.ruleVersionId()))
        .isEqualTo("PENDING_APPROVAL");
  }

  @Test
  void relayPublishesThePendingSnapshotBeforeAdvancingThePublishedVersion() throws Exception {
    UUID ruleId =
        repository
            .createProposal("high-amount", "High amount", definition(), "rule-author")
            .ruleId();
    UUID versionId =
        jdbc.queryForObject(
            "SELECT rule_version_id FROM rules.rule_version WHERE rule_id = ?", UUID.class, ruleId);
    repository.approve(versionId, "rule-approver");
    List<String> published = new ArrayList<>();
    OutboxRelay relay = new OutboxRelay(jdbc, (key, payload) -> published.add(key + ":" + payload));

    assertThat(relay.relayNext()).isTrue();
    assertThat(published).hasSize(1);
    assertThat(published.getFirst()).startsWith("ACTIVE:");
    assertThat(
            jdbc.queryForObject(
                "SELECT published_version FROM rules.ruleset_head WHERE head_key = 'ACTIVE'",
                Long.class))
        .isEqualTo(1L);
    assertThat(
            jdbc.queryForObject(
                "SELECT status FROM rules.outbox_event WHERE aggregate_version = 1", String.class))
        .isEqualTo("PUBLISHED");
  }

  @Test
  void failedFirstPublicationBlocksLaterSnapshotsDuringBackoff() throws Exception {
    UUID ruleId =
        repository
            .createProposal("high-amount", "High amount", definition(), "rule-author")
            .ruleId();
    UUID firstVersion =
        jdbc.queryForObject(
            "SELECT rule_version_id FROM rules.rule_version WHERE rule_id = ?", UUID.class, ruleId);
    repository.approve(firstVersion, "rule-approver");
    UUID secondVersion = repository.proposeChange(ruleId, "UPSERT", definition(), "rule-author");
    repository.approve(secondVersion, "rule-approver");
    List<String> published = new ArrayList<>();
    OutboxRelay relay =
        new OutboxRelay(
            jdbc,
            (key, payload) -> {
              throw new IllegalStateException("broker unavailable");
            });

    assertThat(relay.relayNext()).isFalse();
    assertThat(
            jdbc.queryForObject(
                "SELECT attempt_count FROM rules.outbox_event WHERE aggregate_version = 1",
                Integer.class))
        .isEqualTo(1);
    OutboxRelay retry = new OutboxRelay(jdbc, (key, payload) -> published.add(payload));
    assertThat(retry.relayNext()).isFalse();
    assertThat(published).isEmpty();
    assertThat(
            jdbc.queryForObject(
                "SELECT status FROM rules.outbox_event WHERE aggregate_version = 2", String.class))
        .isEqualTo("PENDING");
  }

  @Test
  void relayRepublishesTheSameSnapshotWhenKafkaAcknowledgesBeforeTheDatabaseMarkFails()
      throws Exception {
    var proposal =
        repository.createProposal("high-amount", "High amount", definition(), "rule-author");
    repository.approve(proposal.ruleVersionId(), "rule-approver");
    List<String> acknowledgedPayloads = new ArrayList<>();
    OutboxRelay firstAttempt =
        new OutboxRelay(
            jdbc,
            (key, payload) -> {
              acknowledgedPayloads.add(key + ":" + payload);
              throw new IllegalStateException(
                  "database mark unavailable after kafka acknowledgement");
            });

    assertThat(firstAttempt.relayNext()).isFalse();
    jdbc.update(
        "UPDATE rules.outbox_event SET next_attempt_at = CURRENT_TIMESTAMP WHERE aggregate_version = 1");
    List<String> retriedPayloads = new ArrayList<>();
    OutboxRelay retry =
        new OutboxRelay(jdbc, (key, payload) -> retriedPayloads.add(key + ":" + payload));

    assertThat(retry.relayNext()).isTrue();
    assertThat(retriedPayloads).containsExactlyElementsOf(acknowledgedPayloads);
    assertThat(
            jdbc.queryForObject(
                "SELECT status FROM rules.outbox_event WHERE aggregate_version = 1", String.class))
        .isEqualTo("PUBLISHED");
  }

  @Test
  void relayUsesProgressiveBackoffForRepeatedPublicationFailures() throws Exception {
    var proposal =
        repository.createProposal("high-amount", "High amount", definition(), "rule-author");
    repository.approve(proposal.ruleVersionId(), "rule-approver");
    OutboxRelay relay =
        new OutboxRelay(
            jdbc,
            (key, payload) -> {
              throw new IllegalStateException("broker unavailable");
            });

    assertThat(relay.relayNext()).isFalse();
    jdbc.update(
        "UPDATE rules.outbox_event SET next_attempt_at = CURRENT_TIMESTAMP WHERE aggregate_version = 1");
    Instant secondAttemptStart = Instant.now();
    assertThat(relay.relayNext()).isFalse();

    assertThat(
            jdbc.queryForObject(
                "SELECT attempt_count FROM rules.outbox_event WHERE aggregate_version = 1",
                Integer.class))
        .isEqualTo(2);
    Instant retryAt =
        jdbc.queryForObject(
                "SELECT next_attempt_at FROM rules.outbox_event WHERE aggregate_version = 1",
                Timestamp.class)
            .toInstant();
    assertThat(retryAt).isAfterOrEqualTo(secondAttemptStart.plusSeconds(2));
  }

  @Test
  void relayDoesNotEmitAnOutboxSnapshotWithAnAlteredContentHash() throws Exception {
    var proposal =
        repository.createProposal("high-amount", "High amount", definition(), "rule-author");
    repository.approve(proposal.ruleVersionId(), "rule-approver");
    jdbc.update(
        """
        UPDATE rules.outbox_event
        SET canonical_payload = replace(canonical_payload, 'sha256:', 'sha256:altered-')
        WHERE aggregate_version = 1
        """);
    List<String> published = new ArrayList<>();
    OutboxRelay relay = new OutboxRelay(jdbc, (key, payload) -> published.add(payload));

    assertThat(relay.relayNext()).isFalse();
    assertThat(published).isEmpty();
    assertThat(
            jdbc.queryForObject(
                "SELECT status FROM rules.outbox_event WHERE aggregate_version = 1", String.class))
        .isEqualTo("PENDING");
  }

  @Test
  void concurrentApprovalsOfTheSameVersionProduceOnePromotionAndOneConflict() throws Exception {
    UUID ruleId =
        repository
            .createProposal("high-amount", "High amount", definition(), "rule-author")
            .ruleId();
    UUID versionId =
        jdbc.queryForObject(
            "SELECT rule_version_id FROM rules.rule_version WHERE rule_id = ?", UUID.class, ruleId);
    CountDownLatch start = new CountDownLatch(1);
    try (var executor = Executors.newFixedThreadPool(2)) {
      var first =
          executor.submit(
              () -> {
                start.await();
                return repository.approve(versionId, "rule-approver-a");
              });
      var second =
          executor.submit(
              () -> {
                start.await();
                return repository.approve(versionId, "rule-approver-b");
              });
      start.countDown();

      assertThat(List.of(first.get(), second.get()))
          .filteredOn(ApprovalResult::approved)
          .hasSize(1);
      assertThat(List.of(first.get(), second.get()))
          .filteredOn(result -> "VERSION_ALREADY_DECIDED".equals(result.denialReason()))
          .hasSize(1);
    }
  }

  @Test
  void concurrentApprovalsOfDifferentRulesCreateConsecutiveSnapshotsWithoutLosingChanges()
      throws Exception {
    var firstProposal = repository.createProposal("high-amount-a", "A", definition(), "author-a");
    var secondProposal = repository.createProposal("high-amount-b", "B", definition(), "author-b");
    CountDownLatch start = new CountDownLatch(1);
    try (var executor = Executors.newFixedThreadPool(2)) {
      var first =
          executor.submit(
              () -> {
                start.await();
                return repository.approve(firstProposal.ruleVersionId(), "approver-a");
              });
      var second =
          executor.submit(
              () -> {
                start.await();
                return repository.approve(secondProposal.ruleVersionId(), "approver-b");
              });
      start.countDown();

      assertThat(List.of(first.get().desiredVersion(), second.get().desiredVersion()))
          .containsExactlyInAnyOrder(1L, 2L);
      assertThat(repository.activeRulesetStatus().desiredVersion()).isEqualTo(2L);
      assertThat(jdbc.queryForObject("SELECT count(*) FROM rules.outbox_event", Integer.class))
          .isEqualTo(2);
    }
  }

  private com.fasterxml.jackson.databind.JsonNode definition() throws Exception {
    return new ObjectMapper()
        .readTree(
            """
        {"type":"AMOUNT_THRESHOLD","amountMinor":10000,"currency":"BRL"}
        """);
  }
}
