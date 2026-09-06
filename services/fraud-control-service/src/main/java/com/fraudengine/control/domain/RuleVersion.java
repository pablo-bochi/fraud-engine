package com.fraudengine.control.domain;

import java.util.UUID;

public record RuleVersion(
    UUID ruleId,
    int versionNumber,
    String changeType,
    String authorSubject,
    String status,
    String approverSubject) {
  public RuleVersion approve(String subject) {
    return decide(subject, "APPROVED");
  }

  public RuleVersion reject(String subject) {
    return decide(subject, "REJECTED");
  }

  private RuleVersion decide(String subject, String decision) {
    if (authorSubject.equals(subject)) {
      throw new IllegalArgumentException("SELF_APPROVAL");
    }
    if (!status.equals("PENDING_APPROVAL")) {
      throw new IllegalStateException("VERSION_ALREADY_DECIDED");
    }
    return new RuleVersion(ruleId, versionNumber, changeType, authorSubject, decision, subject);
  }

  public static RuleVersion propose(
      UUID ruleId, int versionNumber, String changeType, String authorSubject) {
    return new RuleVersion(
        ruleId, versionNumber, changeType, authorSubject, "PENDING_APPROVAL", null);
  }
}
