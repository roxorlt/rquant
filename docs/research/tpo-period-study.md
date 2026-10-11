# TPO period study (15 / 30 / 60 min)

`scripts/tpo_study.py` on the research DB, 2026-10-11: the 300 stocks with 5-minute bars,
~66k next-day observations per period (ex-rights day pairs skipped). Prior-day POC/VAH/VAL
on the same 28-row grid as the stock page; random level = uniform in the prior day's range.

| period | VAH touch / reject | VAL touch / reject | POC touch / reject | random touch / reject | open in VA: reach POC vs mirror | open outside: continue / stay outside |
|---|---|---|---|---|---|---|
| 15 | 60.4% / 50.0% | 65.1% / 49.7% | 69.1% / 50.0% | 56.2% / 49.7% | 85.4% vs 83.6% (+1.8) | 48.1% / 62.5% |
| 30 | 59.2% / 50.0% | 64.8% / 49.4% | 69.4% / 50.0% | 56.6% / 49.5% | 84.9% vs 82.7% (+2.2) | 48.1% / 62.4% |
| 60 | 56.6% / 49.9% | 64.4% / 49.6% | 69.5% / 50.0% | 56.2% / 49.6% | 82.1% vs 79.9% (+2.1) | 48.2% / 62.6% |

Findings: the period barely matters. Prior-day value levels show no reject edge over a random
level (all ≈ 50%) — they are reference levels, not support/resistance. When opening inside
value, price trades through the POC ~2 pp more often than a mirrored level (largest at 30 min).
Opens outside value do not continue on average (48% close further away). Default: 30 min.
