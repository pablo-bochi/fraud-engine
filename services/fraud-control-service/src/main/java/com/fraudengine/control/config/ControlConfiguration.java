package com.fraudengine.control.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fraudengine.control.adapter.out.kafka.KafkaSnapshotPublisher;
import com.fraudengine.control.adapter.out.persistence.JdbcRuleRepository;
import com.fraudengine.control.adapter.out.persistence.OutboxRelay;
import com.fraudengine.control.adapter.out.persistence.SnapshotPublisher;
import com.fraudengine.control.domain.RuleDefinitionValidator;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.EnableScheduling;

@Configuration
@EnableScheduling
public class ControlConfiguration {
  @Bean
  RuleDefinitionValidator ruleDefinitionValidator() {
    return new RuleDefinitionValidator(8, 900);
  }

  @Bean
  JdbcRuleRepository ruleRepository(
      JdbcTemplate jdbc, RuleDefinitionValidator validator, ObjectMapper objectMapper) {
    return new JdbcRuleRepository(jdbc, validator, objectMapper);
  }

  @Bean
  SnapshotPublisher snapshotPublisher(KafkaTemplate<String, String> kafka) {
    return new KafkaSnapshotPublisher(kafka, "fraud.ruleset.active.v1");
  }

  @Bean
  OutboxRelay outboxRelay(JdbcTemplate jdbc, SnapshotPublisher publisher) {
    return new OutboxRelay(jdbc, publisher);
  }
}
