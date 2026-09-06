CREATE TABLE rule (
  rule_id UUID PRIMARY KEY,
  rule_key VARCHAR(100) NOT NULL UNIQUE,
  name VARCHAR(200) NOT NULL,
  created_by_subject VARCHAR(200) NOT NULL,
  created_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE rule_version (
  rule_version_id UUID PRIMARY KEY,
  rule_id UUID NOT NULL REFERENCES rule(rule_id),
  version_number INTEGER NOT NULL CHECK (version_number > 0),
  change_type VARCHAR(20) NOT NULL CHECK (change_type IN ('UPSERT', 'RETIRE')),
  status VARCHAR(30) NOT NULL CHECK (status IN ('PENDING_APPROVAL', 'APPROVED', 'REJECTED')),
  definition JSONB,
  author_subject VARCHAR(200) NOT NULL,
  approver_subject VARCHAR(200),
  decided_at TIMESTAMPTZ,
  decision_reason VARCHAR(500),
  lock_version INTEGER NOT NULL DEFAULT 0,
  CONSTRAINT rule_version_unique_number UNIQUE (rule_id, version_number),
  CONSTRAINT rule_version_different_approver CHECK (approver_subject IS NULL OR approver_subject <> author_subject)
);

CREATE UNIQUE INDEX rule_version_one_pending_per_rule
  ON rule_version(rule_id) WHERE status = 'PENDING_APPROVAL';

CREATE TABLE audit_event (
  audit_event_id UUID PRIMARY KEY,
  actor_subject VARCHAR(200) NOT NULL,
  action VARCHAR(100) NOT NULL,
  outcome VARCHAR(30) NOT NULL,
  entity_type VARCHAR(50) NOT NULL,
  entity_id VARCHAR(200) NOT NULL,
  before_hash VARCHAR(128),
  after_hash VARCHAR(128),
  occurred_at TIMESTAMPTZ NOT NULL
);

CREATE FUNCTION forbid_audit_mutation() RETURNS trigger AS $$
BEGIN
  RAISE EXCEPTION 'audit_event is append-only';
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER audit_event_append_only
  BEFORE UPDATE OR DELETE ON audit_event
  FOR EACH ROW EXECUTE FUNCTION forbid_audit_mutation();
