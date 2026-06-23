# KPI Definition — Realtime Logistics System

## 0. Conventions

| # | Decision | Detail |
|---|---|---|
| 1 | **Realtime ↔ Batch pairing** | Each KPI row gives the streaming source (Kafka event + window) **and** the Iceberg fact it is validated against. |
| 2 | **Terminal state** | A shipment is *completed* on `delivered` **or** `returned_to_sender`. `completed_at := COALESCE(fact_shipment.delivered_at, fact_shipment.returned_at)`. |
| 3 | **Per-shipment stateful KPIs** | The tracking topic is keyed by `facility_id`, so a shipment scatters across partitions. Journey/dwell/timeout KPIs **repartition by `shipment_id`** and order by `sequence_no` + `event_time` under watermark. |
| 4 | **Branch / region rollups** | Financial events carry `branch_id` denormalized (no join). Tracking/shipment events join `dim_facility → dim_branch → dim_region` (broadcast). |
| 5 | **Money** | VND integers, `SUM` only. |
| 6 | **Watermark per topic** | `event_time` tracks **wall-clock** while the lifecycle is `TIME_SCALE`-compressed, so watermarks are **seconds**, not hours. Ceiling = max injected lateness ≈ `0.4 × lifespan` ≈ **120s @2000×**. Default **60s**; swept in §6.1. Override via env `WM_SHIPMENT` / `WM_TRACKING` / `WM_FINANCIAL`. |
| 7 | **Demo time-scale** | `event_generator.py` compresses lifecycle delays by `TIME_SCALE` (default 2000×) → wall-clock lifespan ≈ **90s (delivered) – 300s (returned)**. Business-time thresholds below (e.g. "24h") map to wall-clock ≈ `threshold ÷ TIME_SCALE`. Injected lateness is **scale-aware**: `uniform(0.05, 0.40) × lifespan` per shipment (`--late-frac-lo/-hi`), so it stays proportional when `TIME_SCALE` changes. |
| 8 | **Window labels** | Window sizes named in §1–§5 (e.g. "tumbling 1 h") are *logical* buckets. For the demo, instantiate them from the wall-clock window set in §6.2 (30s / 2 min / 5 min) since `event_time` runs at wall-clock, not business, time. |

---

## 1. Operational KPIs — Realtime

| KPI | Realtime source (event → window) | Group by | Batch ground truth |
|---|---|---|---|
| **Order volume by facility** | `shipment_created` (`logistics.shipment.events`) · tumbling 5 min / 1 h | `pickup_facility_id` | `COUNT(*)` `fact_shipment` GROUP BY `pickup_facility_id`, `date_id` |
| **Order volume by branch / region** | `shipping_fee_confirmed` (`logistics.financial.events`) — carries `branch_id` · tumbling 1 h | `branch_id` → `region_id` | `COUNT(*)` `fact_shipment` ⋈ `dim_facility` GROUP BY `branch_id` |
| **Order volume by partner / service** | `shipment_created` · tumbling 1 h | `partner_id`, `service_type_id` | `COUNT(*)` `fact_shipment` GROUP BY `partner_id`, `service_type_id` |
| **Realtime revenue (intake)** | `SUM(revenue_amount_vnd)` from `shipping_fee_confirmed` · tumbling 5 min / 1 h | `branch_id` | `SUM(revenue_amount_vnd)` `fact_financial_transaction` WHERE `revenue_type='SHIPPING_FEE'` |
| **Realtime COD collected** | `SUM(cod_amount_vnd)` from `cod_collected` · sliding 1 h / slide 5 min | `branch_id` | `SUM(cod_collected_vnd)` `fact_delivery_attempt` WHERE `result='SUCCESS'` |
| **COD collection success rate** | `cod_collected / (cod_collected + cod_failed)` event counts · tumbling 1 h | `branch_id` | `SUM(SUCCESS cod) / SUM(committed cod)` over `fact_delivery_attempt` |
| **In-process order count** | Stateful: shipments with `shipment_created` but no terminal event yet (`flatMapGroupsWithState` keyed by `shipment_id`) · realtime snapshot | global / `current_facility` | `COUNT(*)` `fact_shipment` WHERE `delivered_at IS NULL AND returned_at IS NULL` |

> **Note on revenue source:** `shipping_fee_confirmed` fires 1:1 with `shipment_created` at intake and already contains `branch_id`, so revenue/volume branch rollups need **no** dimension join. Use `shipment_created` only when `pickup_facility_id`-level granularity is required.

---

## 2. Anomaly Detection Rules

All thresholds are **business-time** (divide by `TIME_SCALE` for demo wall-clock). Key
wall-clock values @2000×: stuck **43s** (24h), lost **130s** (72h), long hub dwell
**14s** (480 min). Note lateness ceiling (~120s) sits below the lost timeout, so late
data does not trip false "lost" anomalies; it may briefly exceed the stuck timeout.

| Anomaly | Rule | Realtime implementation |
|---|---|---|
| **Stuck shipment** | No new event for **> 24 h** since last event AND not terminal | `flatMapGroupsWithState` keyed by `shipment_id`, `GroupStateTimeout` = 24 h; emit when timeout fires and last state ∉ {`delivered`, `returned_to_sender`} |
| **Suspected lost** | No event for **> 72 h**, not terminal | Same state machine, 72 h timeout tier |
| **Long hub dwell** | `departed_hub.event_time − arrived_at_hub.event_time` > **480 min** (EXPRESS/SAMEDAY) / **1440 min** (STANDARD) | Repartition by `shipment_id`; pair `arrived_at_hub`→`departed_hub` per `facility_id`; join `dim_service_type.speed_tier` for threshold |
| **Facility congestion** | Backlog (in-flow − out-flow) > **80% `capacity_per_day`** for 2 consecutive 1-h windows | Tumbling 1 h on tracking events ⋈ `dim_facility.capacity_per_day`; flag 2 consecutive breaches |
| **Abnormal route** | Shipment scanned at a hub **not** on its assigned `route_id` | `arrived_at_hub.facility_id` vs `dim_route` (origin/destination hub of `route_id` from `departed_*` events) |
| **Multiple delivery failures** | `≥ 2` `failed_delivery` events for one shipment | Stateful count of `failed_delivery` keyed by `shipment_id` |
| **Dirty / invalid data** | `weight_gram ≤ 0`, NULL required IDs, invalid `*_province_id` (not in `dim_province`), `sla_committed_date` in the past at creation | Inline validation on the Bronze→Silver path; emit to a quality-violation counter (per [simulation.md](../.claude/rules/simulation.md) dirty-data rule) |
| **Excessive lateness** | `processing_time − event_time` exceeds topic watermark → event dropped from windowed aggregates | Observability counter of dropped-late events per topic |

---

## 3. Shipment Journey KPIs

Stateful, **repartitioned by `shipment_id`**. Realtime via paired event timestamps; batch via `fact_shipment` timestamp columns or `fact_shipment_route`.

| KPI | Realtime formula (event_time deltas) | Batch ground truth |
|---|---|---|
| **End-to-end transit (E2E)** | `delivered.event_time − shipment_created.event_time` | `delivered_at − created_at` (`fact_shipment`) |
| **Pickup lead time** | `picked_up − shipment_created` | `pickup_at − created_at` |
| **First-mile time** | `arrived_at_origin_post_office − picked_up` | `origin_post_office_arrived_at − pickup_at` |
| **Origin PO dwell** | `departed_origin_post_office − arrived_at_origin_post_office` | `origin_post_office_departed_at − origin_post_office_arrived_at` |
| **Per-hub dwell** | `departed_hub − arrived_at_hub` (per `facility_id`) | `fact_shipment_route`: `DEPARTED_HUB.event_time − ARRIVED_AT_HUB.event_time` |
| **Linehaul / transit time** | next `arrived_at_hub − departed_hub` (consecutive hubs) | `fact_shipment_route` consecutive `DEPARTED_HUB → ARRIVED_AT_HUB` |
| **Total hub storage** | `SUM` of all per-hub dwell for the shipment | `SUM(dwell)` over hub rows in `fact_shipment_route` |
| **Last-mile / out-for-delivery** | `delivered.event_time − dispatched_for_delivery.event_time` (last attempt) | `delivered_at − out_for_delivery_at` |

---

## 4. SLA & Quality KPIs

| KPI | Realtime formula | Batch ground truth |
|---|---|---|
| **SLA compliance rate** | `COUNT(date(delivered.event_time) ≤ sla_committed_date) / COUNT(delivered) × 100` | `fact_shipment` WHERE `delivered_at IS NOT NULL` |
| **SLA breach count** | `COUNT(date(completed_at) > sla_committed_date)` incl. still-in-transit past SLA | `fact_shipment` WHERE `is_delayed = true` |
| **Delay duration (days)** | `MAX(now, completed_at)::date − sla_committed_date` | `fact_shipment.sla_committed_date` vs `completed_at` |
| **First-attempt success rate** | `COUNT(delivered AND attempt_no=1) / COUNT(DISTINCT shipment_id reaching delivery) × 100` | `fact_delivery_attempt`: `attempt_no=1 AND result='SUCCESS'` / `COUNT(DISTINCT shipment_id)` |
| **Re-delivery rate** | `COUNT(shipment with any attempt_no>1) / COUNT(DISTINCT delivered shipment) × 100` | `fact_delivery_attempt` `MAX(attempt_no)>1` per shipment |
| **Failed-delivery rate** | `COUNT(failed_delivery) / COUNT(dispatched_for_delivery) × 100` | `fact_delivery_attempt`: `result='FAILED'` / total attempts |
| **Return rate** | `COUNT(returned_to_sender) / COUNT(shipment_created) × 100` | `fact_shipment` WHERE `is_returned = true` |
| **Top failure reasons** | `COUNT` GROUP BY `failure_reason_code` (from `failed_delivery`) · tumbling 1 h | `fact_delivery_attempt` WHERE `result='FAILED'` GROUP BY `failure_reason_code` |

---

## 5. Hub / Facility Performance KPIs

| KPI | Realtime source | Batch ground truth |
|---|---|---|
| **Hub throughput (pkgs/hour)** | `COUNT(arrived_at_hub) + COUNT(departed_hub)` per hub · tumbling 1 h | `fact_shipment_route` hub events per hour |
| **Average dwell per hub** | `AVG(departed_hub − arrived_at_hub)` GROUP BY `facility_id` · stateful | `AVG(dwell)` over `fact_shipment_route` GROUP BY `facility_id` |
| **Facility backlog** | in-flow − out-flow per facility (running) | latest `fact_hub_inventory.total_shipments` |
| **Capacity utilization** | backlog ÷ `dim_facility.capacity_per_day × 100` | `fact_hub_inventory.capacity_utilization_pct` |
| **Overdue at facility** | `COUNT` shipments past expected dwell at a facility | `fact_hub_inventory.overdue_shipments` |
| **Facilities with delayed orders** | `COUNT(is_delayed)` GROUP BY current facility | `fact_shipment` GROUP BY `current_facility_id` WHERE `is_delayed=true` |

---

## 6. Experiment Axes (project goal: config comparison)

KPIs are recomputed under varied streaming configs; every run is compared against the
**same** batch ground truth (Iceberg Gold via Athena).

**Time domains (important).** Windows run on `event_time`, which tracks **wall-clock**
(the lifecycle is `TIME_SCALE`-compressed, §0). Therefore:
- **Watermark** and **lateness** are in **seconds** and scale with `TIME_SCALE`.
- **Window size** is chosen by **demo runtime** (how many windows close), independent of `TIME_SCALE`.
- **Trigger interval** is a pure latency/throughput knob, independent of both.

Reference wall-clock @ `TIME_SCALE=2000`: lifespan ≈ 90s (delivered) – 300s (returned);
injected lateness ≈ 4–120s; stuck 43s; lost 130s.

**Method:** sweep one axis at a time, holding the others at **baseline** —
watermark `60s`, tumbling `2 min`, trigger `10s`.

### 6.1 Watermark sweep — deviation vs completeness
Override env `WM_SHIPMENT` / `WM_TRACKING` / `WM_FINANCIAL`.

| Run | Watermark | Late kept | Expected |
|---|---|---|---|
| W1 | 15s | few | max deviation vs batch, min state, min latency |
| W2 | 30s | ~half | |
| W3 | 60s (baseline) | most | small deviation |
| W4 | 120s | ~all (≈0 drop) | matches batch, max state + latency |

*Metrics:* dropped-late count per topic, deviation % vs batch per KPI, RocksDB state size.
*KPIs most affected:* dropped-late count, dwell/E2E completeness, deviation vs batch.

### 6.2 Window sweep — granularity vs stability
(Implemented in the downstream KPI stage, not the Bronze writer in `streaming.py`.)

| Run | Window | Closed in ~30-min demo | Note |
|---|---|---|---|
| WIN-S | tumbling 30s | ~60 | finest, most boundary noise |
| WIN-M | tumbling 2 min (baseline) | ~15 | balanced |
| WIN-L | tumbling 5 min | ~6 | smoothest; + sliding 5 min / 1 min for COD |

*Metrics:* result emission cadence, per-window count stability, edge-effect deviation.
*KPIs most affected:* volume, revenue, COD, throughput.

### 6.3 Trigger sweep — latency vs throughput
Override env `TRIGGER_INTERVAL`.

| Run | Trigger | Expected |
|---|---|---|
| T-S | 1s | lowest latency, most per-batch overhead / small files |
| T-M | 10s (baseline) | balanced |
| T-L | 30s | highest throughput, larger batches, higher latency |

*Metrics:* end-to-end latency p50/p95 (`ingest_time − event_time`), rows/s, files per commit.

### 6.4 Partition strategy

| Variant | KPIs most affected |
|---|---|
| tracking keyed by `facility_id` vs `shipment_id` | per-shipment journey/dwell correctness, facility-level throughput skew |
