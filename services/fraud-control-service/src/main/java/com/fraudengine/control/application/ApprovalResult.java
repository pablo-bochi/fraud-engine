package com.fraudengine.control.application;

import java.util.UUID;

public record ApprovalResult(
    UUID snapshotId, long desiredVersion, String publicationStatus, String denialReason) {
  public static ApprovalResult pending(UUID snapshotId, long desiredVersion) {
    return new ApprovalResult(snapshotId, desiredVersion, "PENDING", null);
  }

  public static ApprovalResult denied(String reason) {
    return new ApprovalResult(null, 0, null, reason);
  }

  public boolean approved() {
    return denialReason == null;
  }
}
