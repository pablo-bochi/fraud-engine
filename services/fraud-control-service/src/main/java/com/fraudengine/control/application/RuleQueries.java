package com.fraudengine.control.application;

import java.util.List;
import java.util.UUID;

public interface RuleQueries {
  RulesetStatus activeRulesetStatus();

  List<RuleSummary> rules();

  List<RuleVersionSummary> versions(UUID ruleId);

  boolean versionBelongsToRule(UUID ruleId, UUID versionId);

  List<AuditEventSummary> auditEvents();

  List<OutboxEventSummary> outboxEvents();
}
