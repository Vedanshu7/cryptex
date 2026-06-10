-- Migration 003: Positions table.
-- Tracks per-tenant, per-symbol portfolio holdings and average cost basis.

BEGIN;

CREATE TABLE IF NOT EXISTS positions (
    id         UUID          PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id  UUID          NOT NULL REFERENCES tenants(id),
    symbol     VARCHAR(20)   NOT NULL,
    quantity   DECIMAL(18,8) NOT NULL DEFAULT 0 CHECK (quantity >= 0),
    avg_price  DECIMAL(18,8) NOT NULL DEFAULT 0 CHECK (avg_price >= 0),
    updated_at TIMESTAMPTZ   NOT NULL DEFAULT NOW(),
    UNIQUE (tenant_id, symbol)
);

CREATE INDEX IF NOT EXISTS idx_positions_tenant
    ON positions (tenant_id);

COMMIT;
