"""Four student-owned boundaries used by the live platform.

Run ``uv run pytest starter-tests -q`` while completing these functions.  Do
not change their signatures: Kafka, Delta, Feast and ``/ready`` call them.
"""

from __future__ import annotations

from collections.abc import Iterable
from typing import Any

from lab28_platform.contracts import FEATURE_REFS, IngestionEvent


def event_headers(
    traceparent: str | None, idempotency_key: str
) -> list[tuple[str, bytes]]:
    """Return byte-valued Kafka headers for trace and replay correlation.

    ``idempotency-key`` is always required.  Omit ``traceparent`` when no trace
    is active rather than sending an empty, invalid W3C header.
    """
    headers = [("idempotency-key", idempotency_key.encode("utf-8"))]
    if traceparent is not None:
        headers.append(("traceparent", traceparent.encode("utf-8")))
    return headers


def dedupe_latest(events: Iterable[IngestionEvent]) -> list[IngestionEvent]:
    """Return one newest event per idempotency key, in deterministic key order.

    Compare ``(occurred_at, event_id)`` so ties do not depend on Kafka delivery
    order.  The Spark Delta MERGE calls this through ``delta_store``.
    """
    latest = {}
    for event in events:
        key = event.idempotency_key
        if key not in latest:
            latest[key] = event
        else:
            current = latest[key]
            if (event.occurred_at, event.event_id) > (current.occurred_at, current.event_id):
                latest[key] = event
    return [latest[k] for k in sorted(latest.keys())]


def feast_online_request(asker_id: str) -> dict[str, Any]:
    """Build the Feast ``/get-online-features`` request for ``asker_activity_v1``."""
    return {
        "features": list(FEATURE_REFS),
        "entities": {"asker_id": [asker_id]},
        "full_feature_names": False,
    }


def readiness_status(probes: Iterable[dict[str, Any]]) -> str:
    """Return ``ready``, ``degraded`` or ``not_ready`` from probe severity."""
    has_mandatory_failure = False
    has_optional_failure = False
    
    for probe in probes:
        is_ready = probe.get("ready", False)
        is_mandatory = probe.get("mandatory", False)
        
        if not is_ready:
            if is_mandatory:
                has_mandatory_failure = True
            else:
                has_optional_failure = True
                
    if has_mandatory_failure:
        return "not_ready"
    if has_optional_failure:
        return "degraded"
    return "ready"
