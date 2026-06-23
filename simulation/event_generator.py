#!/usr/bin/env python3
"""
Streaming event generator — produces logistics events to 3 Kafka topics, exactly
following docs/streaming_events_schema.md.

Topics / partition keys:
  logistics.shipment.events    key = pickup_facility_id
  logistics.tracking.events    key = facility_id
  logistics.financial.events   key = facility_id

ID synchronization: all IDs come from `catalog.build_catalog()`, the same
deterministic catalog that `dim_seeder.py` writes to the Dim tables. Every event
therefore references a Dim row that exists (referential integrity by construction).

Realism: each shipment runs the full waybill lifecycle (created -> pickup ->
linehaul -> delivery / return), compressed by TIME_SCALE so a shipment completes in
~minutes of wall time. ~5% of events are delayed before send to exercise watermarks;
the delay is scale-aware (a fraction of each shipment's wall-clock lifespan).
"""
import argparse
import heapq
import itertools
import json
import os
import random
import time
from datetime import datetime, timedelta, timezone

from catalog import build_catalog

TZ = timezone(timedelta(hours=7))
SCHEMA_VERSION = "v1.0"

TOPIC_SHIPMENT = "logistics.shipment.events"
TOPIC_TRACKING = "logistics.tracking.events"
TOPIC_FINANCIAL = "logistics.financial.events"

FAILURE_REASONS = [
    "RECEIVER_ABSENT", "WRONG_ADDRESS", "RECEIVER_REFUSED", "PHONE_NOT_ANSWERED",
    "ADDRESS_NOT_FOUND", "OUTSIDE_DELIVERY_HOURS", "DAMAGED_PACKAGE",
    "SECURITY_ACCESS_DENIED", "WEATHER_CONDITIONS", "OTHER",
]
RECIPIENT_RELATIONS = ["SELF", "FAMILY", "NEIGHBOR", "SECURITY", "RECEPTION"]
ITEM_CATEGORIES = ["ELECTRONICS", "FASHION", "COSMETICS", "BOOKS", "FOOD",
                   "HOME_APPLIANCE", "TOYS", "DOCUMENTS", "HEALTH", "OTHER"]

_seq_counter = itertools.count(1)

class Refs:
    def __init__(self, rnd: random.Random):
        cat = build_catalog()
        self.rnd = rnd
        self.provinces = {p["province_id"]: p for p in cat["dim_province"]}
        self.branch_of = {f["facility_id"]: f["branch_id"] for f in cat["dim_facility"]}
        self.ftype = {f["facility_id"]: f["facility_type"] for f in cat["dim_facility"]}

        self.po_by_prov: dict[str, list[str]] = {}
        self.hub_by_prov: dict[str, list[str]] = {}
        for f in cat["dim_facility"]:
            tgt = self.po_by_prov if f["facility_type"] == "POST_OFFICE" else self.hub_by_prov
            tgt.setdefault(f["province_id"], []).append(f["facility_id"])
        self.all_hubs = [fid for hubs in self.hub_by_prov.values() for fid in hubs]

        self.shippers_by_fac: dict[str, list[str]] = {}
        for s in cat["dim_shipper"]:
            if s["is_active"]:
                self.shippers_by_fac.setdefault(s["facility_id"], []).append(s["shipper_id"])
        self.all_shippers = [s["shipper_id"] for s in cat["dim_shipper"] if s["is_active"]]

        self.route_by_pair = {(r["origin_hub_id"], r["destination_hub_id"]): r["route_id"]
                              for r in cat["dim_route"]}
        self.all_routes = [r["route_id"] for r in cat["dim_route"]]

        self.services = cat["dim_service_type"]
        self.partners = [p["partner_id"] for p in cat["dim_partner"]]

        self.order_provs = [p for p in self.po_by_prov]
        self.order_weights = [len(self.po_by_prov[p]) for p in self.order_provs]

    def pick_province(self) -> str:
        return self.rnd.choices(self.order_provs, weights=self.order_weights, k=1)[0]

    def pick_po(self, prov: str) -> str:
        return self.rnd.choice(self.po_by_prov[prov])

    def pick_hub(self, prov: str) -> str:
        hubs = self.hub_by_prov.get(prov) or self.all_hubs
        return self.rnd.choice(hubs)

    def pick_route(self, origin_hub: str, dest_hub: str) -> str:
        return self.route_by_pair.get((origin_hub, dest_hub)) or self.rnd.choice(self.all_routes)

    def pick_shipper(self, facility_id: str) -> str:
        return self.rnd.choice(self.shippers_by_fac.get(facility_id) or self.all_shippers)


# Shipment plan
def _mins(rnd, lo, hi):
    """A delay drawn in [lo, hi] minutes, expressed in seconds (real-time, pre-scale)."""
    return rnd.uniform(lo, hi) * 60.0


def plan_shipment(refs: Refs, rnd: random.Random) -> list[dict]:
    """Build the full ordered event list for one shipment.

    Each element: {offset, topic, key, type, fields}. `offset` is seconds from
    creation in *simulated* time (TIME_SCALE applied later).
    """
    sender_prov = refs.pick_province()
    receiver_prov = refs.pick_province()
    pickup_fac = refs.pick_po(sender_prov)
    delivery_fac = refs.pick_po(receiver_prov)
    origin_hub = refs.pick_hub(sender_prov)
    dest_hub = refs.pick_hub(receiver_prov)
    route_id = refs.pick_route(origin_hub, dest_hub)
    shipper = refs.pick_shipper(delivery_fac)

    service = rnd.choice(refs.services)
    partner = rnd.choice(refs.partners)
    same_prov = sender_prov == receiver_prov
    is_remote = refs.provinces[sender_prov]["is_remote"] or refs.provinces[receiver_prov]["is_remote"]

    # Goods + fees
    weight = rnd.choice([300, 500, 800, 1000, 1500, 2000, 3000, 5000])
    declared = rnd.choice([None, rnd.randint(50_000, 3_000_000)])
    shipping_fee = service["base_price_vnd"] + max(0, (weight - 1000)) // 500 * 3000
    fuel = round(shipping_fee * 0.05)
    remote_fee = 15000 if is_remote else 0
    insurance = round(declared * 0.01) if declared else 0
    total_fee = shipping_fee + fuel + remote_fee + insurance
    has_cod = service["supports_cod"] and rnd.random() < 0.6
    cod = (declared or rnd.randint(50_000, 2_000_000)) if has_cod else 0

    # SLA committed days
    if same_prov:
        sla_days = service["sla_inner_city_days"]
    elif is_remote:
        sla_days = service["sla_remote_days"]
    else:
        sla_days = service["sla_inter_province_days"]
    sla_date = (datetime.now(TZ) + timedelta(days=max(1, sla_days))).date().isoformat()

    pickup_branch = refs.branch_of[pickup_fac]
    delivery_branch = refs.branch_of[delivery_fac]

    plan: list[dict] = []
    t = 0.0

    def add(topic, key, etype, fields):
        plan.append({"offset": t, "topic": topic, "key": key, "type": etype, "fields": fields})

    # 1) created (+ shipping fee confirmed)
    add(TOPIC_SHIPMENT, pickup_fac, "shipment_created", {
        "partner_id": partner, "service_type_id": service["service_type_id"],
        "sender_province_id": sender_prov, "sender_district_id": f"{sender_prov}_D01",
        "sender_ward_id": f"{sender_prov}_D01_W01", "pickup_facility_id": pickup_fac,
        "receiver_province_id": receiver_prov, "receiver_district_id": f"{receiver_prov}_D01",
        "receiver_ward_id": f"{receiver_prov}_D01_W01", "delivery_facility_id": delivery_fac,
        "declared_value_vnd": declared, "weight_gram": weight,
        "length_cm": rnd.randint(10, 40), "width_cm": rnd.randint(10, 30),
        "height_cm": rnd.randint(5, 25), "item_category": rnd.choice(ITEM_CATEGORIES),
        "is_fragile": rnd.random() < 0.2, "cod_amount_vnd": cod,
        "shipping_fee_vnd": shipping_fee, "fuel_surcharge_vnd": fuel,
        "remote_area_fee_vnd": remote_fee, "insurance_fee_vnd": insurance,
        "total_fee_vnd": total_fee, "payment_by": rnd.choice(["SENDER", "RECEIVER"]),
        "sla_committed_date": sla_date,
    })
    add(TOPIC_FINANCIAL, pickup_fac, "shipping_fee_confirmed", {
        "facility_id": pickup_fac, "branch_id": pickup_branch, "partner_id": partner,
        "service_type_id": service["service_type_id"], "revenue_type": "SHIPPING_FEE",
        "revenue_amount_vnd": total_fee, "shipping_fee_vnd": shipping_fee,
        "fuel_surcharge_vnd": fuel, "remote_area_fee_vnd": remote_fee,
        "insurance_fee_vnd": insurance,
    })

    t += _mins(rnd, 0, 5)
    add(TOPIC_SHIPMENT, pickup_fac, "pickup_assigned",
        {"pickup_facility_id": pickup_fac, "assigned_shipper_id": refs.pick_shipper(pickup_fac)})

    t += _mins(rnd, 30, 120)
    if rnd.random() < 0.15:
        reweighed = weight + rnd.randint(500, 2000)
    else:
        reweighed = weight + rnd.choice([0, 0, 0, rnd.randint(-100, 300)])
    add(TOPIC_SHIPMENT, pickup_fac, "picked_up",
        {"pickup_facility_id": pickup_fac, "shipper_id": refs.pick_shipper(pickup_fac),
         "reweighed_gram": reweighed})

    new_shipping_fee = service["base_price_vnd"] + max(0, (reweighed - 1000)) // 500 * 3000
    if new_shipping_fee != shipping_fee:
        new_fuel = round(new_shipping_fee * 0.05)
        new_total = new_shipping_fee + new_fuel + remote_fee + insurance
        t += _mins(rnd, 0, 30)
        add(TOPIC_FINANCIAL, pickup_fac, "fee_adjusted", {
            "facility_id": pickup_fac, "branch_id": pickup_branch,
            "partner_id": partner, "service_type_id": service["service_type_id"],
            "old_total_fee_vnd": total_fee, "new_total_fee_vnd": new_total,
            "adjustment_amount_vnd": new_total - total_fee, "reason_code": "REWEIGH"})

    seq = itertools.count(1)

    def track(facility, etype, extra=None):
        f = {"facility_id": facility, "facility_type": refs.ftype[facility], "sequence_no": next(seq)}
        if extra:
            f.update(extra)
        add(TOPIC_TRACKING, facility, etype, f)

    t += _mins(rnd, 60, 240)
    track(pickup_fac, "arrived_at_origin_post_office")
    t += _mins(rnd, 30, 90)
    track(pickup_fac, "departed_origin_post_office", {"route_id": route_id})

    # Origin hub
    t += _mins(rnd, 30, 90)
    track(origin_hub, "arrived_at_hub", {"route_id": route_id})
    t += _mins(rnd, 120, 480)
    track(origin_hub, "sorted_at_hub")
    t += _mins(rnd, 30, 120)
    track(origin_hub, "departed_hub", {"route_id": route_id, "destination_facility_id": dest_hub})

    # Linehaul → destination hub
    t += _mins(rnd, 120, 2880) if not same_prov else _mins(rnd, 60, 240)
    track(dest_hub, "arrived_at_hub", {"route_id": route_id})
    t += _mins(rnd, 120, 480)
    track(dest_hub, "sorted_at_hub")
    t += _mins(rnd, 30, 120)
    track(dest_hub, "departed_hub", {"destination_facility_id": delivery_fac})

    # Destination post office
    t += _mins(rnd, 30, 90)
    track(delivery_fac, "arrived_at_destination_post_office")

    # Delivery attempts — 70% delivered overall, 30% returned
    delivered = rnd.random() < 0.70
    if delivered:
        n_fail = rnd.choices([0, 1, 2], weights=[75, 18, 7], k=1)[0]
    else:
        n_fail = 3
    attempts = n_fail + (1 if delivered else 0)

    for k in range(1, attempts + 1):
        t += _mins(rnd, 30, 120) if k == 1 else _mins(rnd, 720, 1440)
        track(delivery_fac, "dispatched_for_delivery", {"shipper_id": shipper, "attempt_no": k})
        t += _mins(rnd, 30, 90)
        is_last_success = delivered and k == attempts
        if is_last_success:
            track(delivery_fac, "delivered",
                  {"shipper_id": shipper, "attempt_no": k,
                   "recipient_relation": rnd.choice(RECIPIENT_RELATIONS)})
            if cod:
                add(TOPIC_FINANCIAL, delivery_fac, "cod_collected", {
                    "facility_id": delivery_fac, "branch_id": delivery_branch,
                    "partner_id": partner, "service_type_id": service["service_type_id"],
                    "shipper_id": shipper, "attempt_no": k, "cod_amount_vnd": cod})
        else:
            track(delivery_fac, "failed_delivery",
                  {"shipper_id": shipper, "attempt_no": k,
                   "failure_reason_code": rnd.choice(FAILURE_REASONS),
                   "failure_reason_detail": "Khách không nghe máy / không có nhà"})
            if cod:
                add(TOPIC_FINANCIAL, delivery_fac, "cod_failed", {
                    "facility_id": delivery_fac, "branch_id": delivery_branch,
                    "partner_id": partner, "service_type_id": service["service_type_id"],
                    "shipper_id": shipper, "attempt_no": k, "cod_amount_vnd": cod,
                    "reason_code": "DELIVERY_FAILED"})

    if not delivered:
        t += _mins(rnd, 120, 480)
        add(TOPIC_SHIPMENT, pickup_fac, "return_initiated",
            {"facility_id": delivery_fac, "reason_code": "MAX_ATTEMPTS_REACHED"})
        t += _mins(rnd, 120, 1440)
        track(delivery_fac, "returned_to_sender", {"facility_id": delivery_fac})

    return plan


def envelope(shipment_id, etype, event_dt, fields):
    return {
        "event_id": _uuid(),
        "event_type": etype,
        "event_time": event_dt.isoformat(),
        "processing_time": datetime.now(TZ).isoformat(),
        "schema_version": SCHEMA_VERSION,
        "shipment_id": shipment_id,
        **fields,
    }


def _uuid():
    import uuid
    return str(uuid.uuid4())


def make_producer(bootstrap):
    from kafka import KafkaProducer
    return KafkaProducer(
        bootstrap_servers=bootstrap,
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
        key_serializer=lambda k: k.encode() if k else None,
        linger_ms=10, batch_size=65536, compression_type="lz4",
        acks=1, retries=3,
    )


class StdoutProducer:
    """Dry-run sink — prints events instead of producing to Kafka."""
    def send(self, topic, key, value):
        print(f"{topic}\t{key}\t{json.dumps(value, ensure_ascii=False)}")

    def flush(self):
        pass


def run(producer, refs, rnd, rate, duration, scale, late_pct, late_frac_lo, late_frac_hi):
    """Event-time scheduler in wall-clock seconds.

    Each shipment's events are scheduled at created_wall + offset/scale. ~late_pct
    of events are held an extra lateness so processing_time - event_time > 0. The
    lateness is *scale-aware*: a fraction of THIS shipment's wall-clock lifespan
    (uniform(late_frac_lo, late_frac_hi) × span), so it stays proportional to the
    lifecycle when TIME_SCALE changes instead of being a fixed wall-clock delay.
    """
    heap = []  # (emit_wall, tie, shipment_id, topic, key, etype, event_wall, fields)
    tie = itertools.count()
    start = time.time()
    next_create = start
    create_interval = 1.0 / rate
    sent = 0
    created = 0
    accepting = True

    while True:
        now = time.time()

        if accepting and duration and (now - start) >= duration:
            accepting = False
        if accepting and now >= next_create:
            sid = f"VTP{datetime.now(TZ):%y%m%d}{next(_seq_counter):07d}"
            created += 1
            plan = plan_shipment(refs, rnd)
            span_wall = max(ev["offset"] for ev in plan) / scale
            for ev in plan:
                event_wall = now + ev["offset"] / scale
                if rnd.random() < late_pct:
                    lateness = rnd.uniform(late_frac_lo, late_frac_hi) * span_wall
                else:
                    lateness = 0.0
                emit_wall = event_wall + lateness
                heapq.heappush(heap, (emit_wall, next(tie), sid, ev["topic"],
                                      ev["key"], ev["type"], event_wall, ev["fields"]))
            next_create += create_interval

        due = float("inf") if not accepting else now
        while heap and heap[0][0] <= due:
            _, _, sid, topic, key, etype, event_wall, fields = heapq.heappop(heap)
            event_dt = datetime.fromtimestamp(event_wall, TZ)
            producer.send(topic, key=key, value=envelope(sid, etype, event_dt, fields))
            sent += 1
            if sent % 2000 == 0:
                el = now - start
                print(f"[{el:.0f}s] created={created} sent={sent} "
                      f"(~{sent/el:.0f} msg/s) inflight={len(heap)}")

        if not accepting and not heap:
            break
        time.sleep(0.005)

    producer.flush()
    print(f"Done. created={created} sent={sent} in {time.time()-start:.0f}s")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rate", type=float, default=13.0,
                    help="Shipments created per second (each yields ~15 events)")
    ap.add_argument("--duration", type=int, default=0, help="Seconds to keep creating (0=infinite)")
    ap.add_argument("--time-scale", type=float, default=2000.0,
                    help="Compress lifecycle delays by this factor")
    ap.add_argument("--late-pct", type=float, default=0.05, help="Fraction of late events")
    ap.add_argument("--late-frac-lo", type=float, default=0.05,
                    help="Min lateness as a fraction of a shipment's wall-clock lifespan")
    ap.add_argument("--late-frac-hi", type=float, default=0.40,
                    help="Max lateness as a fraction of a shipment's wall-clock lifespan")
    ap.add_argument("--seed", type=int, default=None, help="RNG seed for event randomness")
    ap.add_argument("--dry-run", action="store_true", help="Print to stdout instead of Kafka")
    args = ap.parse_args()

    rnd = random.Random(args.seed)
    print("Building catalog / indexes …")
    refs = Refs(rnd)

    if args.dry_run:
        producer = StdoutProducer()
    else:
        bootstrap = os.environ["KAFKA_BOOTSTRAP"]
        print(f"Connecting to Kafka at {bootstrap} …")
        producer = make_producer(bootstrap)

    print(f"Generating: rate={args.rate} ship/s, scale={args.time_scale}x, "
          f"late={args.late_pct:.0%} ({args.late_frac_lo:.2f}-{args.late_frac_hi:.2f}×lifespan), "
          f"duration={args.duration or 'inf'}")
    run(producer, refs, rnd, args.rate, args.duration, args.time_scale, args.late_pct,
        args.late_frac_lo, args.late_frac_hi)


if __name__ == "__main__":
    main()
