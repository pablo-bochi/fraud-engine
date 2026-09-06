package com.fraudengine.control.adapter.out.persistence;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fraudengine.contracts.schema.SchemaValidator;
import com.fraudengine.control.application.ApprovalResult;
import com.fraudengine.control.application.AuditEventSummary;
import com.fraudengine.control.application.OutboxEventSummary;
import com.fraudengine.control.application.RuleManagement;
import com.fraudengine.control.application.RuleProposal;
import com.fraudengine.control.application.RuleQueries;
import com.fraudengine.control.application.RuleSummary;
import com.fraudengine.control.application.RuleVersionSummary;
import com.fraudengine.control.application.RulesetStatus;
import com.fraudengine.control.domain.RuleDefinitionValidator;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HexFormat;
import java.util.List;
import java.util.UUID;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

public final class JdbcRuleRepository implements RuleManagement, RuleQueries {
  private static final int MAXIMUM_RULES_PER_SNAPSHOT = 100;
  private static final int MAXIMUM_SNAPSHOT_BYTES = 1_048_576;
  private final JdbcTemplate jdbc;
  private final RuleDefinitionValidator definitionValidator;
  private final ObjectMapper objectMapper;
  private final TransactionTemplate transactions;

  public JdbcRuleRepository(
      JdbcTemplate jdbc, RuleDefinitionValidator definitionValidator, ObjectMapper objectMapper) {
    this.jdbc = jdbc;
    this.definitionValidator = definitionValidator;
    this.objectMapper = objectMapper;
    this.transactions =
        new TransactionTemplate(new DataSourceTransactionManager(jdbc.getDataSource()));
  }

  public RuleProposal createProposal(
      String ruleKey, String name, JsonNode definition, String authorSubject) {
    definitionValidator.validate(definition);
    UUID ruleId = UUID.randomUUID();
    UUID versionId = UUID.randomUUID();
    Instant now = Instant.now();
    transactions.executeWithoutResult(
        status -> {
          jdbc.update(
              """
              INSERT INTO rules.rule(rule_id, rule_key, name, created_by_subject, created_at)
              VALUES (?, ?, ?, ?, ?)
              """,
              ruleId,
              ruleKey,
              name,
              authorSubject,
              Timestamp.from(now));
          jdbc.update(
              """
              INSERT INTO rules.rule_version(
                rule_version_id, rule_id, version_number, change_type, status, definition, author_subject)
              VALUES (?, ?, 1, 'UPSERT', 'PENDING_APPROVAL', CAST(? AS jsonb), ?)
              """,
              versionId,
              ruleId,
              json(definition),
              authorSubject);
          jdbc.update(
              """
              INSERT INTO rules.audit_event(
                audit_event_id, actor_subject, action, outcome, entity_type, entity_id, occurred_at)
              VALUES (?, ?, 'RULE_VERSION_CREATED', 'SUCCESS', 'RULE_VERSION', ?, ?)
              """,
              UUID.randomUUID(),
              authorSubject,
              versionId.toString(),
              Timestamp.from(now));
        });
    return new RuleProposal(ruleId, versionId);
  }

  @Override
  public UUID proposeChange(
      UUID ruleId, String changeType, JsonNode definition, String authorSubject) {
    if (!changeType.equals("UPSERT") && !changeType.equals("RETIRE")) {
      throw new IllegalArgumentException("INVALID_CHANGE_TYPE");
    }
    if (changeType.equals("UPSERT")) {
      definitionValidator.validate(definition);
    }
    UUID versionId = UUID.randomUUID();
    Instant now = Instant.now();
    transactions.executeWithoutResult(
        status -> {
          Integer nextVersion =
              jdbc.queryForObject(
                  "SELECT COALESCE(MAX(version_number), 0) + 1 FROM rules.rule_version WHERE rule_id = ?",
                  Integer.class,
                  ruleId);
          jdbc.update(
              """
              INSERT INTO rules.rule_version(
                rule_version_id, rule_id, version_number, change_type, status, definition, author_subject)
              VALUES (?, ?, ?, ?, 'PENDING_APPROVAL', CAST(? AS jsonb), ?)
              """,
              versionId,
              ruleId,
              nextVersion,
              changeType,
              changeType.equals("UPSERT") ? json(definition) : null,
              authorSubject);
          jdbc.update(
              """
              INSERT INTO rules.audit_event(
                audit_event_id, actor_subject, action, outcome, entity_type, entity_id, occurred_at)
              VALUES (?, ?, 'RULE_VERSION_CREATED', 'SUCCESS', 'RULE_VERSION', ?, ?)
              """,
              UUID.randomUUID(),
              authorSubject,
              versionId.toString(),
              Timestamp.from(now));
        });
    return versionId;
  }

  public ApprovalResult approve(UUID versionId, String approverSubject) {
    return transactions.execute(
        status -> {
          StoredRuleVersion proposed = loadVersionForUpdate(versionId);
          if (!proposed.status().equals("PENDING_APPROVAL")) {
            return ApprovalResult.denied("VERSION_ALREADY_DECIDED");
          }
          if (proposed.authorSubject().equals(approverSubject)) {
            audit(approverSubject, "RULE_APPROVAL_DENIED", "DENIED", versionId, Instant.now());
            return ApprovalResult.denied("SELF_APPROVAL");
          }
          Head head = lockHead();
          List<StoredRuleVersion> candidate = candidate(head.desiredSnapshotId(), proposed);
          if (candidate.isEmpty()) {
            audit(approverSubject, "RULE_APPROVAL_DENIED", "DENIED", versionId, Instant.now());
            return ApprovalResult.denied("ACTIVE_RULESET_MUST_NOT_BE_EMPTY");
          }
          if (candidate.size() > MAXIMUM_RULES_PER_SNAPSHOT) {
            throw new IllegalArgumentException("RULESET_SIZE_EXCEEDS_LIMIT");
          }
          candidate.forEach(rule -> definitionValidator.validate(rule.definition()));
          long desiredVersion = head.desiredVersion() + 1;
          UUID snapshotId = UUID.randomUUID();
          Instant now = Instant.now();
          ObjectNode snapshot = snapshot(snapshotId, desiredVersion, versionId, candidate, now);
          String contentHash = hash(snapshot);
          snapshot.put("contentHash", contentHash);
          String canonicalPayload = canonicalJson(snapshot);
          if (canonicalPayload.getBytes(StandardCharsets.UTF_8).length > MAXIMUM_SNAPSHOT_BYTES) {
            throw new IllegalArgumentException("RULESET_PAYLOAD_EXCEEDS_LIMIT");
          }
          if (!SchemaValidator.draft7().validate("ruleset-snapshot-v1", snapshot).isEmpty()) {
            throw new IllegalArgumentException("INVALID_RULESET_SNAPSHOT");
          }
          jdbc.update(
              """
              INSERT INTO rules.ruleset_snapshot(
                snapshot_id, approved_change_rule_version_id, version, content_hash, created_at)
              VALUES (?, ?, ?, ?, ?)
              """,
              snapshotId,
              versionId,
              desiredVersion,
              contentHash,
              Timestamp.from(now));
          for (int index = 0; index < candidate.size(); index++) {
            jdbc.update(
                """
                INSERT INTO rules.ruleset_snapshot_item(snapshot_id, rule_version_id, evaluation_order)
                VALUES (?, ?, ?)
                """,
                snapshotId,
                candidate.get(index).versionId(),
                index);
          }
          jdbc.update(
              """
              UPDATE rules.rule_version
              SET status = 'APPROVED', approver_subject = ?, decided_at = ?, lock_version = lock_version + 1
              WHERE rule_version_id = ? AND status = 'PENDING_APPROVAL'
              """,
              approverSubject,
              Timestamp.from(now),
              versionId);
          jdbc.update(
              """
              UPDATE rules.ruleset_head
              SET desired_snapshot_id = ?, desired_version = ?, lock_version = lock_version + 1
              WHERE head_key = 'ACTIVE'
              """,
              snapshotId,
              desiredVersion);
          jdbc.update(
              """
              INSERT INTO rules.outbox_event(
                outbox_event_id, aggregate_type, aggregate_id, aggregate_version, event_type,
                canonical_payload, status, next_attempt_at, created_at)
              VALUES (?, 'RULESET', 'ACTIVE', ?, 'RULESET_SNAPSHOT_REQUESTED', ?, 'PENDING', ?, ?)
              """,
              UUID.randomUUID(),
              desiredVersion,
              canonicalPayload,
              Timestamp.from(now),
              Timestamp.from(now));
          audit(approverSubject, "RULE_VERSION_APPROVED", "SUCCESS", versionId, now);
          return ApprovalResult.pending(snapshotId, desiredVersion);
        });
  }

  @Override
  public void reject(UUID versionId, String approverSubject) {
    boolean selfRejection =
        Boolean.TRUE.equals(
            transactions.execute(
                status -> {
                  StoredRuleVersion proposed = loadVersionForUpdate(versionId);
                  if (!proposed.status().equals("PENDING_APPROVAL")) {
                    throw new IllegalStateException("VERSION_ALREADY_DECIDED");
                  }
                  if (proposed.authorSubject().equals(approverSubject)) {
                    audit(
                        approverSubject,
                        "RULE_REJECTION_DENIED",
                        "DENIED",
                        versionId,
                        Instant.now());
                    return true;
                  }
                  Instant now = Instant.now();
                  jdbc.update(
                      """
              UPDATE rules.rule_version
              SET status = 'REJECTED', approver_subject = ?, decided_at = ?, lock_version = lock_version + 1
              WHERE rule_version_id = ? AND status = 'PENDING_APPROVAL'
              """,
                      approverSubject,
                      Timestamp.from(now),
                      versionId);
                  audit(approverSubject, "RULE_VERSION_REJECTED", "SUCCESS", versionId, now);
                  return false;
                }));
    if (selfRejection) {
      throw new IllegalArgumentException("SELF_REJECTION");
    }
  }

  @Override
  public RulesetStatus activeRulesetStatus() {
    return jdbc.queryForObject(
        """
        SELECT head.desired_snapshot_id, head.desired_version, head.published_version,
          (SELECT count(*) FROM rules.outbox_event WHERE status = 'PENDING') AS pending_outbox_events
        FROM rules.ruleset_head head
        WHERE head.head_key = 'ACTIVE'
        """,
        (resultSet, rowNum) ->
            new RulesetStatus(
                resultSet.getObject("desired_snapshot_id", UUID.class),
                resultSet.getLong("desired_version"),
                resultSet.getLong("published_version"),
                resultSet.getLong("pending_outbox_events")));
  }

  @Override
  public List<RuleSummary> rules() {
    return jdbc.query(
        "SELECT rule_id, rule_key, name, created_by_subject, created_at FROM rules.rule ORDER BY rule_key",
        (resultSet, rowNum) ->
            new RuleSummary(
                resultSet.getObject("rule_id", UUID.class),
                resultSet.getString("rule_key"),
                resultSet.getString("name"),
                resultSet.getString("created_by_subject"),
                resultSet.getTimestamp("created_at").toInstant()));
  }

  @Override
  public List<RuleVersionSummary> versions(UUID ruleId) {
    return jdbc.query(
        """
        SELECT rule_version_id, version_number, change_type, status, definition, author_subject, approver_subject
        FROM rules.rule_version WHERE rule_id = ? ORDER BY version_number
        """,
        (resultSet, rowNum) ->
            new RuleVersionSummary(
                resultSet.getObject("rule_version_id", UUID.class),
                resultSet.getInt("version_number"),
                resultSet.getString("change_type"),
                resultSet.getString("status"),
                resultSet.getString("definition") == null
                    ? null
                    : tree(resultSet.getString("definition")),
                resultSet.getString("author_subject"),
                resultSet.getString("approver_subject")),
        ruleId);
  }

  @Override
  public boolean versionBelongsToRule(UUID ruleId, UUID versionId) {
    return Boolean.TRUE.equals(
        jdbc.queryForObject(
            """
            SELECT EXISTS(
              SELECT 1 FROM rules.rule_version WHERE rule_id = ? AND rule_version_id = ?)
            """,
            Boolean.class,
            ruleId,
            versionId));
  }

  @Override
  public List<AuditEventSummary> auditEvents() {
    return jdbc.query(
        """
        SELECT audit_event_id, actor_subject, action, outcome, entity_type, entity_id, occurred_at
        FROM rules.audit_event ORDER BY occurred_at DESC, audit_event_id DESC
        """,
        (resultSet, rowNum) ->
            new AuditEventSummary(
                resultSet.getObject("audit_event_id", UUID.class),
                resultSet.getString("actor_subject"),
                resultSet.getString("action"),
                resultSet.getString("outcome"),
                resultSet.getString("entity_type"),
                resultSet.getString("entity_id"),
                resultSet.getTimestamp("occurred_at").toInstant()));
  }

  @Override
  public List<OutboxEventSummary> outboxEvents() {
    return jdbc.query(
        """
        SELECT outbox_event_id, aggregate_version, status, attempt_count, next_attempt_at, last_error_code
        FROM rules.outbox_event ORDER BY aggregate_version
        """,
        (resultSet, rowNum) ->
            new OutboxEventSummary(
                resultSet.getObject("outbox_event_id", UUID.class),
                resultSet.getLong("aggregate_version"),
                resultSet.getString("status"),
                resultSet.getInt("attempt_count"),
                resultSet.getTimestamp("next_attempt_at").toInstant(),
                resultSet.getString("last_error_code")));
  }

  private Head lockHead() {
    return jdbc.queryForObject(
        """
        SELECT desired_snapshot_id, desired_version
        FROM rules.ruleset_head WHERE head_key = 'ACTIVE' FOR UPDATE
        """,
        (resultSet, rowNum) ->
            new Head(
                resultSet.getObject("desired_snapshot_id", UUID.class),
                resultSet.getLong("desired_version")));
  }

  private StoredRuleVersion loadVersionForUpdate(UUID versionId) {
    return jdbc.queryForObject(
        """
        SELECT rule_version_id, rule_id, version_number, change_type, status, definition, author_subject
        FROM rules.rule_version WHERE rule_version_id = ? FOR UPDATE
        """,
        (resultSet, rowNum) ->
            new StoredRuleVersion(
                resultSet.getObject("rule_version_id", UUID.class),
                resultSet.getObject("rule_id", UUID.class),
                resultSet.getInt("version_number"),
                resultSet.getString("change_type"),
                resultSet.getString("status"),
                resultSet.getString("definition") == null
                    ? null
                    : tree(resultSet.getString("definition")),
                resultSet.getString("author_subject")),
        versionId);
  }

  private List<StoredRuleVersion> candidate(UUID currentSnapshotId, StoredRuleVersion proposed) {
    List<StoredRuleVersion> current =
        currentSnapshotId == null
            ? new ArrayList<>()
            : jdbc.query(
                """
                SELECT rv.rule_version_id, rv.rule_id, rv.version_number, rv.change_type, rv.status,
                  rv.definition, rv.author_subject
                FROM rules.ruleset_snapshot_item item
                JOIN rules.rule_version rv ON rv.rule_version_id = item.rule_version_id
                WHERE item.snapshot_id = ? ORDER BY item.evaluation_order
                """,
                (resultSet, rowNum) ->
                    new StoredRuleVersion(
                        resultSet.getObject("rule_version_id", UUID.class),
                        resultSet.getObject("rule_id", UUID.class),
                        resultSet.getInt("version_number"),
                        resultSet.getString("change_type"),
                        resultSet.getString("status"),
                        tree(resultSet.getString("definition")),
                        resultSet.getString("author_subject")),
                currentSnapshotId);
    current.removeIf(rule -> rule.ruleId().equals(proposed.ruleId()));
    if (proposed.changeType().equals("UPSERT")) {
      current.add(proposed);
    }
    return current;
  }

  private ObjectNode snapshot(
      UUID snapshotId,
      long version,
      UUID approvedChangeVersionId,
      List<StoredRuleVersion> rules,
      Instant createdAt) {
    ObjectNode snapshot = objectMapper.createObjectNode();
    snapshot.put("schemaVersion", 1);
    snapshot.put("snapshotId", snapshotId.toString());
    snapshot.put("version", version);
    snapshot.put("approvedChangeRuleVersionId", approvedChangeVersionId.toString());
    ArrayNode items = snapshot.putArray("rules");
    for (int index = 0; index < rules.size(); index++) {
      StoredRuleVersion rule = rules.get(index);
      ObjectNode item = items.addObject();
      item.put("ruleId", rule.ruleId().toString());
      item.put("ruleVersion", rule.versionNumber());
      item.put("evaluationOrder", index);
      item.put("severity", rule.definition().path("severity").asText("HIGH"));
      item.set("definition", rule.definition());
    }
    snapshot.put("createdAt", createdAt.toString());
    return snapshot;
  }

  private String hash(ObjectNode snapshot) {
    try {
      return "sha256:"
          + HexFormat.of()
              .formatHex(
                  MessageDigest.getInstance("SHA-256")
                      .digest(canonicalJson(snapshot).getBytes(StandardCharsets.UTF_8)));
    } catch (NoSuchAlgorithmException error) {
      throw new IllegalStateException(error);
    }
  }

  private String canonicalJson(JsonNode value) {
    try {
      return objectMapper.writeValueAsString(canonical(value));
    } catch (JsonProcessingException error) {
      throw new IllegalArgumentException("CANONICAL_SERIALIZATION_FAILED", error);
    }
  }

  private JsonNode canonical(JsonNode value) {
    if (value.isObject()) {
      ObjectNode ordered = objectMapper.createObjectNode();
      List<String> names = new ArrayList<>();
      value.fieldNames().forEachRemaining(names::add);
      names.stream()
          .sorted(Comparator.naturalOrder())
          .forEach(name -> ordered.set(name, canonical(value.get(name))));
      return ordered;
    }
    if (value.isArray()) {
      ArrayNode ordered = objectMapper.createArrayNode();
      value.forEach(item -> ordered.add(canonical(item)));
      return ordered;
    }
    return value;
  }

  private JsonNode tree(String value) {
    try {
      return objectMapper.readTree(value);
    } catch (JsonProcessingException error) {
      throw new IllegalStateException("PERSISTED_RULE_DEFINITION_INVALID", error);
    }
  }

  private void audit(
      String actorSubject, String action, String outcome, UUID entityId, Instant occurredAt) {
    jdbc.update(
        """
        INSERT INTO rules.audit_event(
          audit_event_id, actor_subject, action, outcome, entity_type, entity_id, occurred_at)
        VALUES (?, ?, ?, ?, 'RULE_VERSION', ?, ?)
        """,
        UUID.randomUUID(),
        actorSubject,
        action,
        outcome,
        entityId.toString(),
        Timestamp.from(occurredAt));
  }

  private record Head(UUID desiredSnapshotId, long desiredVersion) {}

  private record StoredRuleVersion(
      UUID versionId,
      UUID ruleId,
      int versionNumber,
      String changeType,
      String status,
      JsonNode definition,
      String authorSubject) {}

  private String json(JsonNode value) {
    try {
      return objectMapper.writeValueAsString(value);
    } catch (JsonProcessingException error) {
      throw new IllegalArgumentException("INVALID_RULE_DEFINITION", error);
    }
  }
}
