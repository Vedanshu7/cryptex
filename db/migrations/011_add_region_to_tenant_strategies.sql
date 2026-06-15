-- Migration 011: Add region to tenant_strategies
--
-- Each tenant strategy now declares which exchange region it targets.
-- The signal router reads this to route order requests to the correct
-- regional topic (e.g. tokyo.order-requests, sgp.order-requests, eu.order-requests)
-- so OMS and EMS never need to cross region boundaries after this point.
--
-- Valid region values match the regional topic prefixes:
--   tokyo  → Binance (ap-northeast-1)
--   sgp    → Crypto.com (ap-southeast-1)
--   eu     → Deribit (eu-west-1)

BEGIN;

ALTER TABLE tenant_strategies
    ADD COLUMN IF NOT EXISTS region VARCHAR(20) NOT NULL DEFAULT 'tokyo';

COMMENT ON COLUMN tenant_strategies.region IS
    'Target exchange region. Controls which regional Kafka topic '
    'the signal router publishes order requests to. '
    'Valid values: tokyo, sgp, eu';

-- Index for the signal router query: symbol + enabled + region.
CREATE INDEX IF NOT EXISTS idx_tenant_strategies_symbol_region
    ON tenant_strategies (symbol, region, enabled)
    WHERE enabled = TRUE;

COMMIT;
