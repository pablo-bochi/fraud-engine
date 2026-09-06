package com.fraudengine.control.adapter.in.rest;

import com.fraudengine.control.application.AuditEventSummary;
import com.fraudengine.control.application.RuleQueries;
import java.util.List;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/v1/audit-events")
public class AuditController {
  private final RuleQueries queries;

  public AuditController(RuleQueries queries) {
    this.queries = queries;
  }

  @GetMapping
  public List<AuditEventSummary> auditEvents() {
    return queries.auditEvents();
  }
}
