CREATE TABLE delivery (
  notification_request_id VARCHAR(200) PRIMARY KEY,
  alert_id VARCHAR(200) NOT NULL,
  customer_id VARCHAR(200) NOT NULL,
  transaction_id VARCHAR(200) NOT NULL,
  category VARCHAR(100) NOT NULL,
  template_id VARCHAR(100) NOT NULL,
  locale VARCHAR(20) NOT NULL,
  trace_id VARCHAR(200) NOT NULL,
  requested_at TIMESTAMPTZ NOT NULL,

  status VARCHAR(20) NOT NULL
    CHECK (status IN ('PENDING', 'SENDING', 'SENT', 'FAILED')),

  attempt_count INTEGER NOT NULL DEFAULT 0
    CHECK (attempt_count >= 0),

  channel VARCHAR(20),
  provider_reference VARCHAR(500),
  last_error_code VARCHAR(100),

  created_at TIMESTAMPTZ NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL
);
