#!/usr/bin/env python3
"""Report tenant strategies that don't match the current trading universe.

Independent of live signal traffic — this queries every *enabled*
tenant_strategies row directly, so it catches a misconfigured tenant even if
no signal has been generated for their symbol yet (the signal router's
runtime skip only fires reactively, when a signal actually arrives).

Run against the same database and universe.json a deployment uses:
    DATABASE_URL=postgresql://... UNIVERSE_FILE=pipeline/universe.json \\
        PYTHONPATH=pipeline python3 scripts/check_tenant_universe_consistency.py

Exits non-zero if any mismatch is found, so this can run as a periodic health
check (cron, a monitoring job) rather than only ever being run by hand.
"""

import sys

from shared.universe import get_universe
from shared.universe_consistency import find_tenant_universe_mismatches


def main() -> int:
    """Check tenant strategies against the current universe. Returns exit code."""
    universe = get_universe()
    mismatches = find_tenant_universe_mismatches(universe)

    if not mismatches:
        print(f"OK — all enabled tenant strategies match universe version {universe.version}.")
        return 0

    print(
        f"Found {len(mismatches)} tenant strategy mismatch(es) "
        f"against universe version {universe.version}:\n"
    )
    for m in mismatches:
        if m.reason == "unknown_region":
            detail = f"region {m.region!r} does not exist in the universe"
        else:
            detail = f"region {m.region!r}'s exchange does not list symbol {m.symbol!r}"
        print(
            f"  [{m.reason}] tenant={m.tenant_name!r} ({m.tenant_id}) "
            f"symbol={m.symbol} region={m.region} — {detail}"
        )

    return 1


if __name__ == "__main__":
    sys.exit(main())
