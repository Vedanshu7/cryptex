-- Migration 002: Orders table.
-- Tracks the full lifecycle of each trade order from request through fill.

BEGIN;

CREATE TYPE order_status AS ENUM (
    'PENDING',
    'VALIDATED',
    'FILLED',
    'PARTIAL_FILLED',
    'REJECTED',
    'CANCELLED'
);

CREATE TABLE IF NOT EXISTS orders (
    id         UUID          PRIMARY KEY,
    tenant_id  UUID          NOT NULL REFERENCES tenants(id),
    symbol     VARCHAR(20)   NOT NULL,
    side       VARCHAR(4)    NOT NULL CHECK (side IN ('BUY', 'SELL')),
    quantity   DECIMAL(18,8) NOT NULL CHECK (quantity > 0),
    price      DECIMAL(18,8),
    status     order_status  NOT NULL DEFAULT 'PENDING',
    signal_id  VARCHAR(100),
    created_at TIMESTAMPTZ   NOT NULL DEFAULT NOW(),
    filled_at  TIMESTAMPTZ
);

-- Composite indexes for the most common OMS query patterns.
CREATE INDEX IF NOT EXISTS idx_orders_tenant_status
    ON orders (tenant_id, status);

CREATE INDEX IF NOT EXISTS idx_orders_tenant_created
    ON orders (tenant_id, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_orders_tenant_symbol
    ON orders (tenant_id, symbol);

CREATE INDEX IF NOT EXISTS idx_orders_status
    ON orders (status)
    WHERE status IN ('PENDING', 'VALIDATED');

COMMIT;
