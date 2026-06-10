-- Migration 001: Tenants and API keys.
-- Creates the multi-tenant foundation. All other tables reference tenants(id).

BEGIN;

CREATE TABLE IF NOT EXISTS tenants (
    id         UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    name       VARCHAR(100) NOT NULL,
    created_at TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    UNIQUE (name)
);

CREATE TABLE IF NOT EXISTS tenant_api_keys (
    id             UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id      UUID        NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    exchange       VARCHAR(50) NOT NULL DEFAULT 'binance',
    api_key_enc    TEXT        NOT NULL,  -- AES-256 encrypted at application layer.
    secret_key_enc TEXT        NOT NULL,  -- AES-256 encrypted at application layer.
    created_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (tenant_id, exchange)
);

CREATE TABLE IF NOT EXISTS tenant_strategies (
    id            UUID           PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id     UUID           NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    symbol        VARCHAR(20)    NOT NULL,
    position_size DECIMAL(18, 8) NOT NULL,
    enabled       BOOLEAN        NOT NULL DEFAULT TRUE,
    created_at    TIMESTAMPTZ    NOT NULL DEFAULT NOW(),
    UNIQUE (tenant_id, symbol)
);

-- Indexes supporting signal_router query: active strategies by symbol.
CREATE INDEX IF NOT EXISTS idx_tenant_strategies_symbol_enabled
    ON tenant_strategies (symbol, enabled)
    WHERE enabled = TRUE;

COMMIT;
