-- Migration 005: Row Level Security (RLS) policies.
-- Enforces tenant isolation at the database engine level as defence-in-depth.
-- Application sets app.current_tenant_id session variable before any query.
--
-- Local dev: connects as 'trading' (created by POSTGRES_USER env var).
-- Production: create a 'trading_app' role with minimal privileges and update
--             the connection strings to use it instead of 'trading'.

BEGIN;

-- Create the least-privilege role for production use.
-- In local dev this role is unused — the 'trading' superuser is used instead.
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'trading_app') THEN
        CREATE ROLE trading_app NOINHERIT LOGIN PASSWORD 'CHANGE_IN_PRODUCTION';
    END IF;
END $$;

-- Grant the least-privilege role access to tenant-scoped tables.
GRANT SELECT, INSERT, UPDATE, DELETE
    ON tenants, tenant_api_keys, tenant_strategies, orders, positions, candles
    TO trading_app;

GRANT USAGE ON SCHEMA public TO trading_app;

-- Enable RLS on all tenant-scoped tables.
ALTER TABLE orders            ENABLE ROW LEVEL SECURITY;
ALTER TABLE positions         ENABLE ROW LEVEL SECURITY;
ALTER TABLE tenant_strategies ENABLE ROW LEVEL SECURITY;
ALTER TABLE tenant_api_keys   ENABLE ROW LEVEL SECURITY;

-- Tenants table: readable by all application connections, not writable by app role.
ALTER TABLE tenants ENABLE ROW LEVEL SECURITY;

-- RLS policies: app role can only see rows belonging to the current tenant.
-- current_setting('app.current_tenant_id') is set by the application before each query.

DROP POLICY IF EXISTS tenant_isolation ON orders;
CREATE POLICY tenant_isolation ON orders
    FOR ALL TO trading_app
    USING (tenant_id = current_setting('app.current_tenant_id', TRUE)::uuid);

DROP POLICY IF EXISTS tenant_isolation ON positions;
CREATE POLICY tenant_isolation ON positions
    FOR ALL TO trading_app
    USING (tenant_id = current_setting('app.current_tenant_id', TRUE)::uuid);

DROP POLICY IF EXISTS tenant_isolation ON tenant_strategies;
CREATE POLICY tenant_isolation ON tenant_strategies
    FOR ALL TO trading_app
    USING (tenant_id = current_setting('app.current_tenant_id', TRUE)::uuid);

DROP POLICY IF EXISTS tenant_isolation ON tenant_api_keys;
CREATE POLICY tenant_isolation ON tenant_api_keys
    FOR ALL TO trading_app
    USING (tenant_id = current_setting('app.current_tenant_id', TRUE)::uuid);

-- Tenants table: app role can SELECT all rows (needed for tenant resolution middleware).
DROP POLICY IF EXISTS tenants_read ON tenants;
CREATE POLICY tenants_read ON tenants
    FOR SELECT TO trading_app
    USING (TRUE);

-- Candles are not tenant-scoped — no RLS needed.
-- Audit log is append-only by design — not under RLS.

COMMIT;
