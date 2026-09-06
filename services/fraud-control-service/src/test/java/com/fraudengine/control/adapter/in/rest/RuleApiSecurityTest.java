package com.fraudengine.control.adapter.in.rest;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.fraudengine.control.adapter.in.security.ControlSecurityConfiguration;
import com.fraudengine.control.application.ApprovalResult;
import com.fraudengine.control.application.RuleManagement;
import com.fraudengine.control.application.RuleProposal;
import com.fraudengine.control.application.RuleQueries;
import com.fraudengine.control.application.RulesetStatus;
import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jose.JWSHeader;
import com.nimbusds.jose.crypto.RSASSASigner;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.SignedJWT;
import jakarta.servlet.Filter;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.interfaces.RSAPrivateKey;
import java.security.interfaces.RSAPublicKey;
import java.time.Instant;
import java.util.Date;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit.jupiter.web.SpringJUnitWebConfig;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.context.WebApplicationContext;
import org.springframework.web.servlet.config.annotation.EnableWebMvc;

@SpringJUnitWebConfig(RuleApiSecurityTest.HttpFixture.class)
@TestPropertySource(
    properties = {"control.security.issuer=fraud-local", "control.security.audience=fraud-control"})
class RuleApiSecurityTest {
  private static final KeyPair KEYS = keys();
  private static final UUID DECIDED_VERSION_ID = UUID.randomUUID();
  @Autowired WebApplicationContext context;
  @Autowired Filter springSecurityFilterChain;

  @Test
  void readerCanReachTheRulesResourceWithASignedBearerToken() throws Exception {
    mvc()
        .perform(
            get("/api/v1/rules").header("Authorization", "Bearer " + token("reader", "RULE_READ")))
        .andExpect(status().isOk());
  }

  @Test
  void tokenWithoutRuleReadPermissionCannotReachTheRulesResource() throws Exception {
    mvc()
        .perform(
            get("/api/v1/rules").header("Authorization", "Bearer " + token("writer", "RULE_WRITE")))
        .andExpect(status().isForbidden());
  }

  @Test
  void writeAndApprovalPermissionsRemainIndependentForEveryCommand() throws Exception {
    mvc()
        .perform(
            post("/api/v1/rules")
                .header("Authorization", "Bearer " + token("approver", "RULE_APPROVE"))
                .contentType("application/json")
                .content(
                    """
                    {"ruleKey":"high-amount","name":"High amount",
                     "definition":{"type":"AMOUNT_THRESHOLD","amountMinor":10000,"currency":"BRL"}}
                    """))
        .andExpect(status().isForbidden());
    mvc()
        .perform(
            post(
                    "/api/v1/rules/{ruleId}/versions/{versionId}/approval",
                    UUID.randomUUID(),
                    UUID.randomUUID())
                .header("Authorization", "Bearer " + token("writer", "RULE_WRITE")))
        .andExpect(status().isForbidden());
    mvc()
        .perform(
            post(
                    "/api/v1/rules/{ruleId}/versions/{versionId}/rejection",
                    UUID.randomUUID(),
                    UUID.randomUUID())
                .header("Authorization", "Bearer " + token("writer", "RULE_WRITE")))
        .andExpect(status().isForbidden());
  }

  @Test
  void writerCreatesAProposalUsingTheSubjectFromItsJwt() throws Exception {
    String response =
        mvc()
            .perform(
                post("/api/v1/rules")
                    .header("Authorization", "Bearer " + token("jwt-author", "RULE_WRITE"))
                    .contentType("application/json")
                    .content(
                        """
                    {"ruleKey":"high-amount","name":"High amount",
                     "definition":{"type":"AMOUNT_THRESHOLD","amountMinor":10000,"currency":"BRL"}}
                    """))
            .andExpect(status().isCreated())
            .andReturn()
            .getResponse()
            .getContentAsString();
    assertThat(response).contains("ruleId", "ruleVersionId");
    assertThat(HttpFixture.RULES.authorSubject).isEqualTo("jwt-author");
  }

  @Test
  void approverReceivesAcceptedWhenTheSnapshotIsQueuedForPublication() throws Exception {
    mvc()
        .perform(
            post(
                    "/api/v1/rules/{ruleId}/versions/{versionId}/approval",
                    UUID.randomUUID(),
                    UUID.randomUUID())
                .header("Authorization", "Bearer " + token("jwt-approver", "RULE_APPROVE")))
        .andExpect(status().isAccepted());
  }

  @Test
  void creationRejectsAnAdministrativeIdentitySuppliedByTheClient() throws Exception {
    mvc()
        .perform(
            post("/api/v1/rules")
                .header("Authorization", "Bearer " + token("jwt-author", "RULE_WRITE"))
                .contentType("application/json")
                .content(
                    """
                    {"ruleKey":"high-amount","name":"High amount","authorSubject":"forged",
                     "definition":{"type":"AMOUNT_THRESHOLD","amountMinor":10000,"currency":"BRL"}}
                    """))
        .andExpect(status().isBadRequest());
  }

  @Test
  void approverCanRejectAPendingProposal() throws Exception {
    mvc()
        .perform(
            post(
                    "/api/v1/rules/{ruleId}/versions/{versionId}/rejection",
                    UUID.randomUUID(),
                    UUID.randomUUID())
                .header("Authorization", "Bearer " + token("jwt-approver", "RULE_APPROVE")))
        .andExpect(status().isNoContent());
  }

  @Test
  void writerCanCreateARetirementProposalUsingItsJwtSubject() throws Exception {
    UUID ruleId = UUID.randomUUID();
    mvc()
        .perform(
            post("/api/v1/rules/{ruleId}/versions", ruleId)
                .header("Authorization", "Bearer " + token("jwt-author", "RULE_WRITE"))
                .contentType("application/json")
                .content("{\"changeType\":\"RETIRE\"}"))
        .andExpect(status().isCreated());
    assertThat(HttpFixture.RULES.changeRuleId).isEqualTo(ruleId);
    assertThat(HttpFixture.RULES.changeAuthorSubject).isEqualTo("jwt-author");
  }

  @Test
  void aSecondPendingProposalIsReportedAsAConflict() throws Exception {
    mvc()
        .perform(
            post("/api/v1/rules")
                .header("Authorization", "Bearer " + token("jwt-author", "RULE_WRITE"))
                .contentType("application/json")
                .content(
                    """
                    {"ruleKey":"duplicate","name":"High amount",
                     "definition":{"type":"AMOUNT_THRESHOLD","amountMinor":10000,"currency":"BRL"}}
                    """))
        .andExpect(status().isConflict());
  }

  @Test
  void approvingAnAlreadyDecidedVersionIsReportedAsAConflict() throws Exception {
    mvc()
        .perform(
            post(
                    "/api/v1/rules/{ruleId}/versions/{versionId}/approval",
                    UUID.randomUUID(),
                    DECIDED_VERSION_ID)
                .header("Authorization", "Bearer " + token("jwt-approver", "RULE_APPROVE")))
        .andExpect(status().isConflict());
  }

  @Test
  void approvalRejectsAVersionThatDoesNotBelongToTheRuleInThePath() throws Exception {
    HttpFixture.VERSION_BELONGS_TO_RULE = false;
    try {
      mvc()
          .perform(
              post(
                      "/api/v1/rules/{ruleId}/versions/{versionId}/approval",
                      UUID.randomUUID(),
                      UUID.randomUUID())
                  .header("Authorization", "Bearer " + token("jwt-approver", "RULE_APPROVE")))
          .andExpect(status().isNotFound());
    } finally {
      HttpFixture.VERSION_BELONGS_TO_RULE = true;
    }
  }

  @Test
  void readerCanInspectDesiredAndPublishedRulesetVersions() throws Exception {
    String response =
        mvc()
            .perform(
                get("/api/v1/rulesets/active")
                    .header("Authorization", "Bearer " + token("reader", "RULE_READ")))
            .andExpect(status().isOk())
            .andReturn()
            .getResponse()
            .getContentAsString();
    assertThat(response).contains("desiredVersion", "publishedVersion", "pendingOutboxEvents");
  }

  @Test
  void auditRequiresItsOwnPermission() throws Exception {
    mvc()
        .perform(
            get("/api/v1/audit-events")
                .header("Authorization", "Bearer " + token("reader", "RULE_READ")))
        .andExpect(status().isForbidden());
    mvc()
        .perform(
            get("/api/v1/audit-events")
                .header("Authorization", "Bearer " + token("auditor", "AUDIT_READ")))
        .andExpect(status().isOk());
  }

  private MockMvc mvc() {
    return MockMvcBuilders.webAppContextSetup(context)
        .addFilters(springSecurityFilterChain)
        .build();
  }

  private static String token(String subject, String... permissions) throws Exception {
    SignedJWT jwt =
        new SignedJWT(
            new JWSHeader(JWSAlgorithm.RS256),
            new JWTClaimsSet.Builder()
                .issuer("fraud-local")
                .audience("fraud-control")
                .subject(subject)
                .expirationTime(Date.from(Instant.now().plusSeconds(300)))
                .claim("permissions", List.of(permissions))
                .build());
    jwt.sign(new RSASSASigner((RSAPrivateKey) KEYS.getPrivate()));
    return jwt.serialize();
  }

  private static KeyPair keys() {
    try {
      var generator = KeyPairGenerator.getInstance("RSA");
      generator.initialize(2048);
      return generator.generateKeyPair();
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
  }

  @Configuration
  @EnableWebMvc
  @Import({
    ControlSecurityConfiguration.class,
    RuleController.class,
    RuleQueryController.class,
    RulesetStatusController.class,
    AuditController.class,
    RuleApiExceptionHandler.class
  })
  static class HttpFixture {
    static final CapturingRuleManagement RULES = new CapturingRuleManagement();

    @Bean
    RSAPublicKey publicKey() {
      return (RSAPublicKey) KEYS.getPublic();
    }

    @Bean
    RuleManagement ruleManagement() {
      return RULES;
    }

    @Bean
    RuleQueries ruleQueries() {
      return new RuleQueries() {
        @Override
        public RulesetStatus activeRulesetStatus() {
          return new RulesetStatus(UUID.randomUUID(), 3, 2, 1);
        }

        @Override
        public java.util.List<com.fraudengine.control.application.RuleSummary> rules() {
          return java.util.List.of();
        }

        @Override
        public java.util.List<com.fraudengine.control.application.RuleVersionSummary> versions(
            UUID ruleId) {
          return java.util.List.of();
        }

        @Override
        public boolean versionBelongsToRule(UUID ruleId, UUID versionId) {
          return VERSION_BELONGS_TO_RULE;
        }

        @Override
        public java.util.List<com.fraudengine.control.application.AuditEventSummary> auditEvents() {
          return java.util.List.of();
        }

        @Override
        public java.util.List<com.fraudengine.control.application.OutboxEventSummary>
            outboxEvents() {
          return java.util.List.of();
        }
      };
    }

    static boolean VERSION_BELONGS_TO_RULE = true;
  }

  static class CapturingRuleManagement implements RuleManagement {
    String authorSubject;
    UUID changeRuleId;
    String changeAuthorSubject;

    @Override
    public RuleProposal createProposal(
        String ruleKey,
        String name,
        com.fasterxml.jackson.databind.JsonNode definition,
        String authorSubject) {
      if (ruleKey.equals("duplicate")) {
        throw new DataIntegrityViolationException("rule_version_one_pending_per_rule");
      }
      this.authorSubject = authorSubject;
      return new RuleProposal(UUID.randomUUID(), UUID.randomUUID());
    }

    @Override
    public UUID proposeChange(
        UUID ruleId,
        String changeType,
        com.fasterxml.jackson.databind.JsonNode definition,
        String authorSubject) {
      changeRuleId = ruleId;
      changeAuthorSubject = authorSubject;
      return UUID.randomUUID();
    }

    @Override
    public ApprovalResult approve(UUID versionId, String approverSubject) {
      if (versionId.equals(DECIDED_VERSION_ID)) {
        return ApprovalResult.denied("VERSION_ALREADY_DECIDED");
      }
      return ApprovalResult.pending(UUID.randomUUID(), 1);
    }

    @Override
    public void reject(UUID versionId, String approverSubject) {}
  }
}
