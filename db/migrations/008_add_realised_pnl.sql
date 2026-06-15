-- Migration 008: Add realised_pnl column to positions.
-- Tracks cumulative realised P&L from closed (SELL) fills.

BEGIN;

ALTER TABLE positions
    ADD COLUMN IF NOT EXISTS realised_pnl DECIMAL(18,8) NOT NULL DEFAULT 0;

COMMIT;
