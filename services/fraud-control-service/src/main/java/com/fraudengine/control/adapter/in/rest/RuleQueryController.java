package com.fraudengine.control.adapter.in.rest;

import com.fraudengine.control.application.RuleQueries;
import com.fraudengine.control.application.RuleSummary;
import com.fraudengine.control.application.RuleVersionSummary;
import java.util.List;
import java.util.UUID;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/v1/rules")
public class RuleQueryController {
  private final RuleQueries queries;

  public RuleQueryController(RuleQueries queries) {
    this.queries = queries;
  }

  @GetMapping
  public List<RuleSummary> rules() {
    return queries.rules();
  }

  @GetMapping("/{ruleId}/versions")
  public List<RuleVersionSummary> versions(@PathVariable UUID ruleId) {
    return queries.versions(ruleId);
  }
}
