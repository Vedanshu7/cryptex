-- Migration 007: Executions table.
-- Append-only record of every exchange interaction from the EMS.
-- Not tenant-scoped via RLS — EMS writes as the migrations role.

BEGIN;

CREATE TABLE IF NOT EXISTS executions (
    id               UUID          PRIMARY KEY,
    order_id         UUID          NOT NULL REFERENCES orders(id),
    tenant_id        UUID          NOT NULL REFERENCES tenants(id),
    exchange_order_id BIGINT,
    fill_price       DECIMAL(18,8) NOT NULL DEFAULT 0,
    status           VARCHAR(20)   NOT NULL,
    error_message    TEXT,
    executed_at      TIMESTAMPTZ   NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_executions_order_id
    ON executions (order_id);

CREATE INDEX IF NOT EXISTS idx_executions_tenant_executed
    ON executions (tenant_id, executed_at DESC);

COMMIT;
