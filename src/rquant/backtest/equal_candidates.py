"""Convert a verified screen receipt to an unranked equal-all candidate slice."""

from __future__ import annotations

from rquant.backtest.contracts import RankingSnapshot
from rquant.backtest.screen_calendar_source import VerifiedScreenCalendarCandidates
from rquant.pool_result_receipt import member_price_digest, member_set_digest
from rquant.portfolio.weights import PortfolioCandidate, PortfolioWeightRule
from rquant.runtime_contracts import canonical_sha256


class EqualCandidateRankingError(ValueError):
    """The verified screen set cannot be used as an equal-all ranking input."""


def build_equal_weight_ranking(
    evidence: VerifiedScreenCalendarCandidates,
    weight_rule: PortfolioWeightRule,
) -> RankingSnapshot:
    """Keep all receipt candidates with neutral scores, without a rank claim."""
    screen = evidence.screen
    receipt = screen.receipt
    if receipt.contract != "screen-run-receipt/v2" or receipt.price_digest is None:
        raise EqualCandidateRankingError("a screen receipt v2 price proof is required")
    if (
        not receipt.lineage_complete
        or receipt.trade_date != screen.source_trade_date
        or receipt.preset_name != screen.preset_name
        or receipt.hit_count != len(screen.candidates)
    ):
        raise EqualCandidateRankingError("screen receipt does not bind the candidate set")
    try:
        codes = [item.ts_code for item in screen.candidates]
        prices = [(item.ts_code, item.previous_close) for item in screen.candidates]
        member_digest = member_set_digest(codes)
        price_digest = member_price_digest(prices)
    except ValueError as exc:
        raise EqualCandidateRankingError("screen candidate proof is invalid") from exc
    if receipt.member_digest != member_digest:
        raise EqualCandidateRankingError("screen receipt member digest differs")
    if receipt.price_digest != price_digest:
        raise EqualCandidateRankingError("screen receipt price digest differs")
    if weight_rule.method != "equal":
        raise EqualCandidateRankingError("only equal weighting has evidence from a screen receipt")
    if weight_rule.max_positions < receipt.hit_count:
        raise EqualCandidateRankingError("all candidates must fit; top-N lacks ranking evidence")
    candidates = tuple(PortfolioCandidate(ts_code=item.ts_code) for item in screen.candidates)
    if any(
        candidate.ts_code != item.ts_code
        for candidate, item in zip(candidates, screen.candidates, strict=True)
    ):
        raise EqualCandidateRankingError("screen receipt needs canonical codes for the portfolio")
    return RankingSnapshot(
        source_identity=canonical_sha256(
            {
                "source_mode": "verified_screen_equal_all",
                "screen_receipt": receipt,
                "calendar_source_identity": evidence.calendar.source_identity,
                "source_trade_date": screen.source_trade_date,
                "decision_trade_date": screen.decision_trade_date,
                "preset_name": screen.preset_name,
                "candidate_rows": screen.candidates,
            }
        ),
        source_trade_date=screen.source_trade_date,
        observed_at=screen.completed_at,
        candidates=candidates,
    )
