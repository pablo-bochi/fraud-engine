package com.fraudengine.control.application;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.UUID;

public interface RuleManagement {
  RuleProposal createProposal(
      String ruleKey, String name, JsonNode definition, String authorSubject);

  UUID proposeChange(UUID ruleId, String changeType, JsonNode definition, String authorSubject);

  ApprovalResult approve(UUID versionId, String approverSubject);

  void reject(UUID versionId, String approverSubject);
}
