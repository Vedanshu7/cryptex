-- Idempotency guard: one order per (tenant, signal).
-- If the same Kafka message is delivered twice, the second INSERT fails
-- with a unique_violation (23505) which PlaceOrderHandler catches and skips.
CREATE UNIQUE INDEX IF NOT EXISTS uq_orders_tenant_signal
    ON orders (tenant_id, signal_id)
    WHERE signal_id IS NOT NULL;
