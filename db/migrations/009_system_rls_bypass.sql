-- Migration 009: Add RLS bypass policy for cross-tenant system operations.
--
-- The reconciliation service needs to SELECT stuck orders across all tenants.
-- Rather than a superuser connection or BYPASSRLS privilege, we use a dedicated
-- RLS policy that activates when the application signals a system operation via
-- a session variable. The service sets this inside an explicit transaction using
-- SET LOCAL so it never leaks into concurrent connections.

BEGIN;

DROP POLICY IF EXISTS system_operations ON orders;
CREATE POLICY system_operations ON orders
    FOR ALL TO trading_app
    USING (current_setting('app.is_system_operation', TRUE) = 'true');

DROP POLICY IF EXISTS system_operations ON positions;
CREATE POLICY system_operations ON positions
    FOR ALL TO trading_app
    USING (current_setting('app.is_system_operation', TRUE) = 'true');

COMMIT;
