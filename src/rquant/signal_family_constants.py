"""Frozen leaf constants for the current signal family and its declaration domain.

Amended per Codex round-2 order 2026-08-25, ruling 2. This module is a **leaf**: it imports
nothing from `rquant`, so any module may import it at module scope. That is the whole reason
it exists. `rquant.signal_family_successor_registry` needs module-level constants to spell
`Literal` closed sets, and it previously deferred an import of `rquant.signal_contracts`
inside a function body precisely to avoid an import cycle.

What is frozen here:

* the closed set of three current-family transport channels and the class each one binds;
* the single accepted current family ID;
* the successor and overlay namespaces, and the exact five verification pair IDs;
* the grammar, length bound, and exact role domain of a declaration-level participant
  service ID, plus which role may produce or consume each channel.

**Scope of the service-ID freeze.** This domain governs the Phase B *declaration* schemas in
`rquant.signal_family_successor_registry`. It is deliberately **not** applied to
`RuntimeServiceManifest.service_id` or to `PairBindingV1`: the live production profile in
`rquant.runtime_production_profile` issues IDs such as `signal-router.all-strategies.v1` and
`strategy.n_shape.v1`, which contain dots and underscores and therefore do not satisfy
`SERVICE_ID_PATTERN`. Propagating this grammar upward would be a production topology change,
not a declaration freeze, and needs its own authorization.

Widening any set below requires a new ADR: the successor bundle, the staged overlay, and
their conflict audit identities are all content-addressed over exactly these values.
"""

from __future__ import annotations

import re
from collections.abc import Mapping, Sequence
from types import MappingProxyType
from typing import Final, Literal

# ---------------------------------------------------------------------------------------
# Family
# ---------------------------------------------------------------------------------------

#: The current-family envelope discriminant. Restated here rather than imported so this
#: module stays a leaf; `rquant.signal_contracts.CURRENT_ENVELOPE_SCHEMA` is the contract-side
#: spelling and a red test pins the two to be equal, so they cannot drift apart.
CURRENT_ENVELOPE_FAMILY_ID: Final[str] = "rquant.signal-envelope/v1"

AcceptedFamilyId = Literal["rquant.signal-envelope/v1"]

#: Frozen to exactly one current family. Legacy envelopes carry no such discriminant and can
#: never appear here; accepting a second family requires a new ADR.
ACCEPTED_FAMILY_IDS: Final[tuple[str, ...]] = (CURRENT_ENVELOPE_FAMILY_ID,)

# ---------------------------------------------------------------------------------------
# Channels
# ---------------------------------------------------------------------------------------

SIGNAL_BUS_ROUTED_RECORD_CHANNEL: Final[str] = "signal-bus-routed-record/current"
SIGNAL_ENVELOPE_CHANNEL: Final[str] = "signal-envelope/current"
SIGNAL_ROUTE_SPOOL_RECORD_CHANNEL: Final[str] = "signal-route-spool-record/current"

SuccessorChannelId = Literal[
    "signal-bus-routed-record/current",
    "signal-envelope/current",
    "signal-route-spool-record/current",
]

#: The closed channel set. Each value names a class that already exists, and none of the keys
#: shares an identifier with the frozen v2 `runtime.*` catalog.
SUCCESSOR_CHANNEL_BINDINGS: Final[Mapping[str, str]] = MappingProxyType(
    {
        SIGNAL_BUS_ROUTED_RECORD_CHANNEL: (
            "rquant.signal_route_spool.CurrentSignalBusRoutedRecord"
        ),
        SIGNAL_ENVELOPE_CHANNEL: "rquant.signal_contracts.CurrentSignalEnvelope",
        SIGNAL_ROUTE_SPOOL_RECORD_CHANNEL: (
            "rquant.signal_route_spool.CurrentSignalRouteSpoolRecord"
        ),
    }
)

# ---------------------------------------------------------------------------------------
# Identities
# ---------------------------------------------------------------------------------------

#: A successor bundle's identity is its namespace and nothing else: exactly one successor
#: base may be registered per process, so a second bundle with different bytes is a conflict.
SUCCESSOR_BUNDLE_NAMESPACE: Final[str] = "rquant.signal-family.successor"

#: An overlay's identity is `(OVERLAY_NAMESPACE, base_bundle_content_hash)`: one overlay per
#: successor base, so the same namespace over a different base is a different identity.
OVERLAY_NAMESPACE: Final[str] = "rquant.signal-family.overlay"

#: The exact receipt pair IDs of the later verification phase.
PAIR_IDS: Final[tuple[str, ...]] = (
    "notifier-serving",
    "router-notifier",
    "router-paper",
    "strategy-router",
    "strategy-shadow",
)

# ---------------------------------------------------------------------------------------
# Participant service IDs: grammar, namespace, role domain
# ---------------------------------------------------------------------------------------

#: Lowercase dash-separated segments of ASCII alphanumerics, first segment starting with a
#: letter. No uppercase, underscore, dot, empty segment, leading or trailing dash.
SERVICE_ID_PATTERN: Final[str] = r"^[a-z][a-z0-9]*(-[a-z0-9]+)*$"
SERVICE_ID_MAX_LENGTH: Final[int] = 64

_SERVICE_ID_RE: Final[re.Pattern[str]] = re.compile(SERVICE_ID_PATTERN)

STRATEGY_LIVE_ROLE: Final[str] = "strategy_live"

#: The dynamic strategy namespace. A `strategy_live` service ID is this prefix plus at least
#: one more grammar-legal segment, because the number of strategies is not frozen.
STRATEGY_SERVICE_ID_PREFIX: Final[str] = "strategy-live-"

#: The five Phase C singleton roles and their exact declaration-domain service IDs.
PHASE_C_ROLE_SERVICE_IDS: Final[Mapping[str, str]] = MappingProxyType(
    {
        "notifier": "notifier",
        "paper_broker": "paper-broker",
        "serving_publisher": "serving-publisher",
        "shadow_session": "shadow-session",
        "signal_router": "signal-router",
    }
)

_ROLE_BY_SERVICE_ID: Final[Mapping[str, str]] = MappingProxyType(
    {service_id: role for role, service_id in PHASE_C_ROLE_SERVICE_IDS.items()}
)

#: Which role may produce each channel, derived from the frozen five pair rows: strategy
#: services produce envelopes; the router produces routed and spool records.
CHANNEL_PRODUCER_ROLES: Final[Mapping[str, tuple[str, ...]]] = MappingProxyType(
    {
        SIGNAL_BUS_ROUTED_RECORD_CHANNEL: ("signal_router",),
        SIGNAL_ENVELOPE_CHANNEL: (STRATEGY_LIVE_ROLE,),
        SIGNAL_ROUTE_SPOOL_RECORD_CHANNEL: ("signal_router",),
    }
)

#: Which role may consume each channel, from the same five rows: the router and the shadow
#: session read envelopes (`strategy-router`, `strategy-shadow`); the notifier and the paper
#: broker read what the router published (`router-notifier`, `router-paper`).
CHANNEL_CONSUMER_ROLES: Final[Mapping[str, tuple[str, ...]]] = MappingProxyType(
    {
        SIGNAL_BUS_ROUTED_RECORD_CHANNEL: ("notifier", "paper_broker"),
        SIGNAL_ENVELOPE_CHANNEL: ("shadow_session", "signal_router"),
        SIGNAL_ROUTE_SPOOL_RECORD_CHANNEL: ("notifier", "paper_broker"),
    }
)


def service_id_role(service_id: str) -> str | None:
    """The declaration-domain role of one service ID, or `None` when it has none.

    Grammar-illegal IDs have no role. A `strategy-live-` prefixed ID is the dynamic
    strategy domain; everything else must be one of the five exact Phase C role IDs.
    """

    if type(service_id) is not str:
        return None
    if len(service_id) > SERVICE_ID_MAX_LENGTH or _SERVICE_ID_RE.match(service_id) is None:
        return None
    if service_id.startswith(STRATEGY_SERVICE_ID_PREFIX):
        return STRATEGY_LIVE_ROLE if len(service_id) > len(STRATEGY_SERVICE_ID_PREFIX) else None
    return _ROLE_BY_SERVICE_ID.get(service_id)


def require_service_id_grammar(service_id: str, *, field: str) -> None:
    """Reject any participant ID outside the frozen grammar or length bound."""

    if type(service_id) is not str:
        raise TypeError(f"{field} requires exact service id strings")
    if len(service_id) > SERVICE_ID_MAX_LENGTH:
        raise ValueError(
            f"{field} service id exceeds {SERVICE_ID_MAX_LENGTH} characters: {service_id}"
        )
    if _SERVICE_ID_RE.match(service_id) is None:
        raise ValueError(f"{field} service id is outside the frozen grammar: {service_id}")


def require_channel_role_domain(
    channel_id: str,
    service_ids: Sequence[str],
    *,
    field: str,
    direction: Literal["produce", "consume"],
) -> None:
    """Reject any participant outside this channel's exact producer or consumer role domain.

    Cross-domain substitution is the case this exists for: a consumer-only role standing in
    the producer tuple, a producer-only role standing in the consumer tuple, an unknown role,
    or a case/underscore variant of a legal ID all fail here rather than being accepted as
    an arbitrary nonempty string.
    """

    roles = (CHANNEL_PRODUCER_ROLES if direction == "produce" else CHANNEL_CONSUMER_ROLES).get(
        channel_id
    )
    if roles is None:
        raise ValueError(f"unknown successor transport channel: {channel_id}")
    for service_id in service_ids:
        require_service_id_grammar(service_id, field=field)
        role = service_id_role(service_id)
        if role is None:
            raise ValueError(
                f"{field} service id is outside the frozen role domain: {service_id}"
            )
        if role not in roles:
            raise ValueError(
                f"{field} role {role} cannot {direction} {channel_id}: {service_id}"
            )
