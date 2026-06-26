from __future__ import annotations

import os
from datetime import datetime, timezone

# Thresholds
STUCK_TIMEOUT_S = int(os.environ.get("STUCK_TIMEOUT_S", "43"))
LOST_TIMEOUT_S = int(os.environ.get("LOST_TIMEOUT_S", "130"))
HUB_DWELL_EXPRESS_S = int(os.environ.get("HUB_DWELL_EXPRESS_S", "14"))
HUB_DWELL_STANDARD_S = int(os.environ.get("HUB_DWELL_STANDARD_S", "43"))

TERMINAL_EVENTS = {"delivered", "returned_to_sender"}
EXPRESS_TIERS = {"EXPRESS", "SAMEDAY"}

STATE_FIELDS = [
    "created_at", "picked_up_at", "origin_po_arrived_at", "origin_po_departed_at",
    "last_dispatch_at", "current_hub_id", "current_hub_arrived_at", "total_hub_dwell_s",
    "last_event_time", "last_event_type", "current_facility", "failure_count",
    "max_attempt_no", "speed_tier", "sla_committed_date", "stuck_emitted",
]

OUTPUT_FIELDS = [
    "record_type", "shipment_id",
    # SHIPMENT 
    "status", "current_facility", "e2e_transit_s", "pickup_lead_s", "first_mile_s",
    "origin_po_dwell_s", "total_hub_dwell_s", "last_mile_s", "sla_committed_date",
    "delivered_date", "sla_met", "sla_breach", "attempt_count", "first_attempt_success",
    "is_redelivery", "updated_at",
    # ANOMALY
    "facility_id", "anomaly_type", "severity", "detail", "detected_at",
]

EVENT_FIELDS = [
    "event_type", "event_time", "sequence_no", "facility_id", "pickup_facility_id",
    "route_id", "attempt_no", "service_type_id", "sla_committed_date",
]


def _init_state() -> dict:
    st = {f: None for f in STATE_FIELDS}
    st.update(total_hub_dwell_s=0.0, failure_count=0, max_attempt_no=0, stuck_emitted=False)
    return st


def _init_output() -> dict:
    return {f: None for f in OUTPUT_FIELDS}


def _anomaly(rows: list, sid, facility, atype, severity, detail, at_ms) -> None:
    r = _init_output()
    r.update(record_type="ANOMALY", shipment_id=sid, facility_id=facility,
             anomaly_type=atype, severity=severity, detail=detail, detected_at=at_ms)
    rows.append(r)


def _status_row(rows: list, sid, status, facility, at_ms) -> None:
    r = _init_output()
    r.update(record_type="SHIPMENT", shipment_id=sid, status=status,
             current_facility=facility, updated_at=at_ms)
    rows.append(r)


def _delta_s(end, start) -> float | None:
    return None if end is None or start is None else (end - start) / 1000.0


def _date(ms) -> str | None:
    if ms is None:
        return None
    return datetime.fromtimestamp(ms / 1000.0, tz=timezone.utc).strftime("%Y-%m-%d")


def _completed_row(sid: str, st: dict, event_type: str, completed_at) -> dict:
    delivered = event_type == "delivered"
    attempt = st["max_attempt_no"] or None
    delivered_date = _date(completed_at) if delivered else None
    sla = st["sla_committed_date"]
    completed_date = _date(completed_at)

    r = _init_output()
    r.update(
        record_type="SHIPMENT", shipment_id=sid,
        status="DELIVERED" if delivered else "RETURNED",
        current_facility=st["current_facility"],
        e2e_transit_s=_delta_s(completed_at, st["created_at"]),
        pickup_lead_s=_delta_s(st["picked_up_at"], st["created_at"]),
        first_mile_s=_delta_s(st["origin_po_arrived_at"], st["picked_up_at"]),
        origin_po_dwell_s=_delta_s(st["origin_po_departed_at"], st["origin_po_arrived_at"]),
        total_hub_dwell_s=st["total_hub_dwell_s"],
        last_mile_s=_delta_s(completed_at, st["last_dispatch_at"]) if delivered else None,
        sla_committed_date=sla,
        delivered_date=delivered_date,
        sla_met=int(delivered and sla is not None and delivered_date <= sla),
        sla_breach=int(sla is not None and completed_date is not None and completed_date > sla),
        attempt_count=attempt,
        first_attempt_success=int(delivered and (attempt or 1) == 1),
        is_redelivery=int((attempt or 1) > 1),
        updated_at=completed_at,
    )
    return r


def process_batch(
    sid: str,
    state: dict | None,
    events: list[dict],
    timed_out: bool,
    route_dest: dict[str, str],
    speed_tier: dict[str, str],
) -> tuple[list[dict], dict | None, int | None]:
    """Fold a batch of events (or a timeout) for one shipment.

    Returns (output_rows, new_state, timeout_target_ms). `new_state is None` means the
    state should be removed; `timeout_target_ms is None` means no timeout to (re)arm.
    """
    rows: list[dict] = []

    if timed_out:
        st = state
        last = st["last_event_time"] or 0
        fac = st["current_facility"]
        if not st["stuck_emitted"]:
            at = last + STUCK_TIMEOUT_S * 1000
            _anomaly(rows, sid, fac, "STUCK_SHIPMENT", "WARNING",
                     f"no event for >{STUCK_TIMEOUT_S}s; last={st['last_event_type']}", at)
            _status_row(rows, sid, "STUCK", fac, at)
            st["stuck_emitted"] = True
            return rows, st, last + LOST_TIMEOUT_S * 1000
        at = last + LOST_TIMEOUT_S * 1000
        _anomaly(rows, sid, fac, "SUSPECTED_LOST", "CRITICAL",
                 f"no event for >{LOST_TIMEOUT_S}s; last={st['last_event_type']}", at)
        _status_row(rows, sid, "LOST", fac, at)
        return rows, None, None

    st = state if state is not None else _init_state()
    events = sorted(
        events,
        key=lambda e: (
            e["event_time"] if e["event_time"] is not None else float("inf"),
            e["sequence_no"] if e["sequence_no"] is not None else float("inf"),
        ),
    )

    terminal = None  # (event_type, event_time_ms)
    for ev in events:
        event_type = ev["event_type"]
        event_time = ev["event_time"]
        fac = ev.get("facility_id") or ev.get("pickup_facility_id")
        attempt = ev.get("attempt_no")

        if event_time is not None and (st["last_event_time"] is None or event_time >= st["last_event_time"]):
            st["last_event_time"] = event_time
            st["last_event_type"] = event_type
            st["stuck_emitted"] = False  
        if fac is not None:
            st["current_facility"] = fac

        if event_type == "shipment_created":
            if st["created_at"] is None:
                st["created_at"] = event_time
            st["sla_committed_date"] = ev.get("sla_committed_date")
            st["speed_tier"] = speed_tier.get(ev.get("service_type_id"), "STANDARD")
        elif event_type == "picked_up":
            st["picked_up_at"] = event_time
        elif event_type == "arrived_at_origin_post_office":
            st["origin_po_arrived_at"] = event_time
        elif event_type == "departed_origin_post_office":
            st["origin_po_departed_at"] = event_time
        elif event_type == "arrived_at_hub":
            st["current_hub_id"] = fac
            st["current_hub_arrived_at"] = event_time
            rid = ev.get("route_id")
            if rid is not None and fac is not None:
                expected = route_dest.get(rid)
                if expected is not None and expected != fac:
                    _anomaly(rows, sid, fac, "ABNORMAL_ROUTE", "WARNING",
                             f"arrived {fac}, route {rid} expects {expected}", event_time)
        elif event_type == "departed_hub":
            if st["current_hub_arrived_at"] is not None and event_time is not None:
                dwell = (event_time - st["current_hub_arrived_at"]) / 1000.0
                st["total_hub_dwell_s"] += dwell
                thr = (HUB_DWELL_EXPRESS_S if st.get("speed_tier") in EXPRESS_TIERS
                       else HUB_DWELL_STANDARD_S)
                if dwell > thr:
                    _anomaly(rows, sid, st["current_hub_id"], "LONG_HUB_DWELL", "WARNING",
                             f"dwell {dwell:.0f}s > {thr}s ({st.get('speed_tier')})", event_time)
                st["current_hub_arrived_at"] = None
        elif event_type == "dispatched_for_delivery":
            st["last_dispatch_at"] = event_time
            if attempt is not None:
                st["max_attempt_no"] = max(st["max_attempt_no"], attempt)
        elif event_type == "delivered":
            if attempt is not None:
                st["max_attempt_no"] = max(st["max_attempt_no"], attempt)
            terminal = ("delivered", event_time)
        elif event_type == "failed_delivery":
            st["failure_count"] += 1
            if attempt is not None:
                st["max_attempt_no"] = max(st["max_attempt_no"], attempt)
            if st["failure_count"] == 2:
                _anomaly(rows, sid, fac, "MULTIPLE_FAILURES", "WARNING", "2nd failed_delivery", event_time)
        elif event_type == "returned_to_sender":
            terminal = ("returned_to_sender", event_time)

    if terminal is not None:
        rows.append(_completed_row(sid, st, terminal[0], terminal[1]))
        return rows, None, None

    _status_row(rows, sid, "IN_PROCESS", st["current_facility"], st["last_event_time"])
    target = (st["last_event_time"] or 0) + (
        LOST_TIMEOUT_S if st["stuck_emitted"] else STUCK_TIMEOUT_S) * 1000
    return rows, st, target
