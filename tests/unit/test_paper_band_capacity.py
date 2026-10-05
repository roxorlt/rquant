"""One frozen maximum-capacity run, with a separately known zero-return reference."""

from decimal import Decimal
import json
import resource
import time


def test_full_2520_day_budget_has_exact_zero_reference_and_bounded_memory() -> None:
    from rquant.paper_portfolio_band import BOOTSTRAP_PATHS, bootstrap_daily_band

    started = time.monotonic()
    before = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    points = bootstrap_daily_band((Decimal("0"),) * 2520, days=2520)
    after = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    assert len(points) == 2520
    assert tuple(point.day_index for point in points) == tuple(range(1, 2521))
    assert all(point.lower == Decimal(1) and point.upper == Decimal(1) for point in points)
    # macOS reports bytes, Linux KiB. The delta remains below the original
    # worker's 512 MiB budget on either platform; no path-by-day matrix is needed.
    import sys

    delta_bytes = (after - before) * (1 if sys.platform == "darwin" else 1024)
    assert delta_bytes < 128 * 1024 * 1024
    print(json.dumps({"capacity_days": 2520, "paths": BOOTSTRAP_PATHS,
                      "draws": 2520 * BOOTSTRAP_PATHS,
                      "reference": "Every zero-return path has NAV=1 at every day.",
                      "all_5040_quantiles_exact": True, "rss_delta_bytes": delta_bytes,
                      "elapsed_seconds": time.monotonic() - started}, sort_keys=True))
