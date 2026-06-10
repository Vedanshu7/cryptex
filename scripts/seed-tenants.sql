-- Seed script: creates two test tenants with active strategies.
-- Run after migrations to have data for local development.

BEGIN;

INSERT INTO tenants (id, name) VALUES
    ('11111111-1111-1111-1111-111111111111', 'Alice Trading'),
    ('22222222-2222-2222-2222-222222222222', 'Bob Capital')
ON CONFLICT (name) DO NOTHING;

-- Alice trades BTCUSDT and ETHUSDT.
INSERT INTO tenant_strategies (tenant_id, symbol, position_size, enabled) VALUES
    ('11111111-1111-1111-1111-111111111111', 'BTCUSDT', 0.001, TRUE),
    ('11111111-1111-1111-1111-111111111111', 'ETHUSDT', 0.01,  TRUE)
ON CONFLICT (tenant_id, symbol) DO UPDATE SET enabled = TRUE;

-- Bob only trades BTCUSDT.
INSERT INTO tenant_strategies (tenant_id, symbol, position_size, enabled) VALUES
    ('22222222-2222-2222-2222-222222222222', 'BTCUSDT', 0.002, TRUE)
ON CONFLICT (tenant_id, symbol) DO UPDATE SET enabled = TRUE;

-- Initialize flat positions for all strategies.
INSERT INTO positions (tenant_id, symbol, quantity, avg_price) VALUES
    ('11111111-1111-1111-1111-111111111111', 'BTCUSDT', 0, 0),
    ('11111111-1111-1111-1111-111111111111', 'ETHUSDT', 0, 0),
    ('22222222-2222-2222-2222-222222222222', 'BTCUSDT', 0, 0)
ON CONFLICT (tenant_id, symbol) DO NOTHING;

COMMIT;
