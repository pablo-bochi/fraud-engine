package com.fraudengine.control.application;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.UUID;
import org.junit.jupiter.api.Test;

class RuleApprovalTest {
  @Test
  void aQueuedApprovalExposesTheSnapshotAndPendingPublicationState() {
    UUID snapshotId = UUID.randomUUID();

    ApprovalResult result = ApprovalResult.pending(snapshotId, 4);

    assertThat(result.approved()).isTrue();
    assertThat(result.snapshotId()).isEqualTo(snapshotId);
    assertThat(result.desiredVersion()).isEqualTo(4);
    assertThat(result.publicationStatus()).isEqualTo("PENDING");
    assertThat(result.denialReason()).isNull();
  }

  @Test
  void aDeniedApprovalDoesNotExposeAQueuedSnapshot() {
    ApprovalResult result = ApprovalResult.denied("SELF_APPROVAL");

    assertThat(result.approved()).isFalse();
    assertThat(result.snapshotId()).isNull();
    assertThat(result.publicationStatus()).isNull();
    assertThat(result.denialReason()).isEqualTo("SELF_APPROVAL");
  }
}
