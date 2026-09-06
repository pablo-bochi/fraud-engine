package com.fraudengine.control.adapter.in.rest;

import com.fraudengine.control.application.OutboxEventSummary;
import com.fraudengine.control.application.RuleQueries;
import com.fraudengine.control.application.RulesetStatus;
import java.util.List;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/v1/rulesets")
public class RulesetStatusController {
  private final RuleQueries queries;

  public RulesetStatusController(RuleQueries queries) {
    this.queries = queries;
  }

  @GetMapping("/active")
  public RulesetStatus active() {
    return queries.activeRulesetStatus();
  }

  @GetMapping("/active/outbox")
  public List<OutboxEventSummary> outbox() {
    return queries.outboxEvents();
  }
}
