package com.fraudengine.control.adapter.in.rest;

import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.databind.JsonNode;
import com.fraudengine.control.application.ApprovalResult;
import com.fraudengine.control.application.RuleManagement;
import com.fraudengine.control.application.RuleProposal;
import com.fraudengine.control.application.RuleQueries;
import java.util.Map;
import java.util.UUID;
import org.springframework.http.HttpStatus;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

@RestController
@RequestMapping("/api/v1/rules")
public class RuleController {
  private final RuleManagement rules;
  private final RuleQueries queries;

  public RuleController(RuleManagement rules, RuleQueries queries) {
    this.rules = rules;
    this.queries = queries;
  }

  @PostMapping
  @org.springframework.web.bind.annotation.ResponseStatus(HttpStatus.CREATED)
  public Map<String, UUID> create(
      @RequestBody CreateRuleRequest request, @AuthenticationPrincipal Jwt jwt) {
    RuleProposal proposal =
        rules.createProposal(
            request.ruleKey(), request.name(), request.definition(), jwt.getSubject());
    return Map.of("ruleId", proposal.ruleId(), "ruleVersionId", proposal.ruleVersionId());
  }

  @PostMapping("/{ruleId}/versions")
  @org.springframework.web.bind.annotation.ResponseStatus(HttpStatus.CREATED)
  public Map<String, UUID> proposeChange(
      @PathVariable UUID ruleId,
      @RequestBody ChangeRuleRequest request,
      @AuthenticationPrincipal Jwt jwt) {
    return Map.of(
        "ruleVersionId",
        rules.proposeChange(ruleId, request.changeType(), request.definition(), jwt.getSubject()));
  }

  @PostMapping("/{ruleId}/versions/{versionId}/approval")
  @org.springframework.web.bind.annotation.ResponseStatus(HttpStatus.ACCEPTED)
  public ApprovalResponse approve(
      @PathVariable UUID ruleId, @PathVariable UUID versionId, @AuthenticationPrincipal Jwt jwt) {
    requireVersionBelongsToRule(ruleId, versionId);
    ApprovalResult result = rules.approve(versionId, jwt.getSubject());
    if (!result.approved()) {
      if (result.denialReason().equals("VERSION_ALREADY_DECIDED")) {
        throw new ResponseStatusException(HttpStatus.CONFLICT, result.denialReason());
      }
      throw new ResponseStatusException(HttpStatus.UNPROCESSABLE_ENTITY, result.denialReason());
    }
    return new ApprovalResponse(
        result.snapshotId(), result.desiredVersion(), result.publicationStatus());
  }

  @PostMapping("/{ruleId}/versions/{versionId}/rejection")
  @org.springframework.web.bind.annotation.ResponseStatus(HttpStatus.NO_CONTENT)
  public void reject(
      @PathVariable UUID ruleId, @PathVariable UUID versionId, @AuthenticationPrincipal Jwt jwt) {
    requireVersionBelongsToRule(ruleId, versionId);
    rules.reject(versionId, jwt.getSubject());
  }

  private void requireVersionBelongsToRule(UUID ruleId, UUID versionId) {
    if (!queries.versionBelongsToRule(ruleId, versionId)) {
      throw new ResponseStatusException(HttpStatus.NOT_FOUND, "RULE_VERSION_NOT_FOUND");
    }
  }

  public static class CreateRuleRequest {
    private String ruleKey;
    private String name;
    private JsonNode definition;

    public String ruleKey() {
      return ruleKey;
    }

    public void setRuleKey(String ruleKey) {
      this.ruleKey = ruleKey;
    }

    public String name() {
      return name;
    }

    public void setName(String name) {
      this.name = name;
    }

    public JsonNode definition() {
      return definition;
    }

    public void setDefinition(JsonNode definition) {
      this.definition = definition;
    }

    @JsonAnySetter
    void rejectUnknownField(String fieldName, JsonNode ignoredValue) {
      throw new ResponseStatusException(
          HttpStatus.BAD_REQUEST, "UNKNOWN_REQUEST_FIELD:" + fieldName);
    }
  }

  public static class ChangeRuleRequest {
    private String changeType;
    private JsonNode definition;

    public String changeType() {
      return changeType;
    }

    public void setChangeType(String changeType) {
      this.changeType = changeType;
    }

    public JsonNode definition() {
      return definition;
    }

    public void setDefinition(JsonNode definition) {
      this.definition = definition;
    }

    @JsonAnySetter
    void rejectUnknownField(String fieldName, JsonNode ignoredValue) {
      throw new ResponseStatusException(
          HttpStatus.BAD_REQUEST, "UNKNOWN_REQUEST_FIELD:" + fieldName);
    }
  }

  public record ApprovalResponse(UUID snapshotId, long desiredVersion, String publicationStatus) {}
}
