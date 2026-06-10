-- Migration 006: Audit log and signal history.
-- Audit log is intentionally NOT under RLS — support teams need cross-tenant access.
-- Signal history is global (market-wide), not tenant-scoped.

BEGIN;

CREATE TABLE IF NOT EXISTS audit_log (
    id          UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id   UUID,        -- NULL for system-level events.
    actor       VARCHAR(100),
    action      VARCHAR(100) NOT NULL,  -- 'ORDER_PLACED', 'ORDER_FILLED', etc.
    entity_id   UUID,
    entity_type VARCHAR(50),
    payload     JSONB,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Support query: all events for a tenant ordered by time.
CREATE INDEX IF NOT EXISTS idx_audit_tenant_created
    ON audit_log (tenant_id, created_at DESC);

-- System-level query: all events of a given action type.
CREATE INDEX IF NOT EXISTS idx_audit_action_created
    ON audit_log (action, created_at DESC);

-- Signal history: archive of all ML-generated signals.
CREATE TABLE IF NOT EXISTS signal_history (
    id           VARCHAR(100)  PRIMARY KEY,
    symbol       VARCHAR(20)   NOT NULL,
    side         VARCHAR(4)    NOT NULL,
    confidence   DECIMAL(5,4)  NOT NULL,
    generated_at TIMESTAMPTZ   NOT NULL,
    expires_at   TIMESTAMPTZ   NOT NULL,
    created_at   TIMESTAMPTZ   NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_signal_history_symbol_generated
    ON signal_history (symbol, generated_at DESC);

COMMIT;
