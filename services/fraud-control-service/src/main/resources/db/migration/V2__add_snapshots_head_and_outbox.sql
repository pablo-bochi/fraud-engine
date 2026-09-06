CREATE TABLE ruleset_snapshot (
  snapshot_id UUID PRIMARY KEY,
  approved_change_rule_version_id UUID NOT NULL REFERENCES rule_version(rule_version_id),
  version BIGINT NOT NULL UNIQUE CHECK (version > 0),
  content_hash VARCHAR(71) NOT NULL,
  created_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE ruleset_snapshot_item (
  snapshot_id UUID NOT NULL REFERENCES ruleset_snapshot(snapshot_id),
  rule_version_id UUID NOT NULL REFERENCES rule_version(rule_version_id),
  evaluation_order INTEGER NOT NULL CHECK (evaluation_order >= 0),
  PRIMARY KEY (snapshot_id, rule_version_id)
);

CREATE TABLE ruleset_head (
  head_key VARCHAR(20) PRIMARY KEY,
  desired_snapshot_id UUID REFERENCES ruleset_snapshot(snapshot_id),
  desired_version BIGINT NOT NULL DEFAULT 0,
  published_version BIGINT NOT NULL DEFAULT 0,
  lock_version INTEGER NOT NULL DEFAULT 0,
  CHECK (published_version <= desired_version)
);

INSERT INTO ruleset_head(head_key) VALUES ('ACTIVE');

CREATE TABLE outbox_event (
  outbox_event_id UUID PRIMARY KEY,
  aggregate_type VARCHAR(50) NOT NULL,
  aggregate_id VARCHAR(200) NOT NULL,
  aggregate_version BIGINT NOT NULL,
  event_type VARCHAR(100) NOT NULL,
  canonical_payload TEXT NOT NULL,
  status VARCHAR(20) NOT NULL CHECK (status IN ('PENDING', 'PUBLISHED')),
  attempt_count INTEGER NOT NULL DEFAULT 0,
  next_attempt_at TIMESTAMPTZ NOT NULL,
  last_error_code VARCHAR(100),
  created_at TIMESTAMPTZ NOT NULL,
  published_at TIMESTAMPTZ,
  UNIQUE (aggregate_type, aggregate_version)
);
