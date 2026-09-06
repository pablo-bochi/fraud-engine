package com.fraudengine.control.domain;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.UUID;
import org.junit.jupiter.api.Test;

class RuleLifecycleTest {
  @Test
  void anotherSubjectRejectsAProposalAndClosesItsDecision() {
    RuleVersion proposal = RuleVersion.propose(UUID.randomUUID(), 1, "RETIRE", "author");

    RuleVersion rejected = proposal.reject("approver");

    assertThat(rejected.status()).isEqualTo("REJECTED");
    assertThat(rejected.approverSubject()).isEqualTo("approver");
    assertThat(rejected.changeType()).isEqualTo("RETIRE");
    assertThatThrownBy(() -> rejected.approve("another")).isInstanceOf(IllegalStateException.class);
  }

  @Test
  void anApprovedVersionCannotBeDecidedAgain() {
    RuleVersion approved =
        RuleVersion.propose(UUID.randomUUID(), 1, "UPSERT", "author").approve("approver");

    assertThatThrownBy(() -> approved.approve("another-approver"))
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("VERSION_ALREADY_DECIDED");
  }

  @Test
  void authorCannotApproveTheirOwnProposal() {
    RuleVersion proposal = RuleVersion.propose(UUID.randomUUID(), 1, "UPSERT", "author");

    assertThatThrownBy(() -> proposal.approve("author"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("SELF_APPROVAL");
  }

  @Test
  void anotherSubjectApprovesWithoutMutatingTheOriginalProposal() {
    RuleVersion proposal = RuleVersion.propose(UUID.randomUUID(), 1, "UPSERT", "author");

    RuleVersion approved = proposal.approve("approver");

    assertThat(approved.status()).isEqualTo("APPROVED");
    assertThat(approved.approverSubject()).isEqualTo("approver");
    assertThat(approved.ruleId()).isEqualTo(proposal.ruleId());
    assertThat(approved.authorSubject()).isEqualTo("author");
    assertThat(proposal.status()).isEqualTo("PENDING_APPROVAL");
  }

  @Test
  void newProposalStartsPendingWithItsRuleAndAuthor() {
    UUID ruleId = UUID.randomUUID();

    RuleVersion proposal = RuleVersion.propose(ruleId, 1, "UPSERT", "rule-author");

    assertThat(proposal.ruleId()).isEqualTo(ruleId);
    assertThat(proposal.versionNumber()).isEqualTo(1);
    assertThat(proposal.changeType()).isEqualTo("UPSERT");
    assertThat(proposal.authorSubject()).isEqualTo("rule-author");
    assertThat(proposal.status()).isEqualTo("PENDING_APPROVAL");
    assertThat(proposal.approverSubject()).isNull();
  }
}
