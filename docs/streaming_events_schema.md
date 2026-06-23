# Streaming Events Schema — Realtime Logistics System

## 1. Common Envelope (every event)

Every message — regardless of topic — carries these fields. Event-specific fields are merged at the top level (flat payload, no nesting) unless noted.

| Field | Type | Nullable | Meaning |
|---|---|---|---|
| `event_id` | string (UUID v4) | NOT NULL | Idempotency key. Dedup key in Silver. |
| `event_type` | string | NOT NULL | `snake_case` event name. |
| `event_time` | string (ISO8601 +07:00) | NOT NULL | When the event **occurred**. Used for watermark. |
| `processing_time` | string (ISO8601 +07:00) | NOT NULL | When produced to Kafka. `processing_time - event_time` = lateness. |
| `schema_version` | string | NOT NULL | `"v1.0"`. |
| `shipment_id` | string | NOT NULL | The waybill this event belongs to. Join key across all topics. |

> **Late data:** 5% of events have `processing_time` delayed (per simulation rule) to exercise watermark behavior. Lateness is observable as `processing_time - event_time`.

---

## 2. Topic `logistics.shipment.events` — Lifecycle / status changes

- **Partition key:** `pickup_facility_id` (hash) → all lifecycle events of a shipment land on the **same** partition (pickup facility is fixed), preserving per-shipment order in this topic.
- **Partitions:** 32 · **Rate:** 200–500 msg/s
- **Feeds:** `fact_shipment` (static + status timestamps)

### 2.1 `shipment_created` (fat event)

| Field | Type | Null | → Dim / Fact |
|---|---|---|---|
| `partner_id` | string | NO | `dim_partner` |
| `service_type_id` | string | NO | `dim_service_type` |
| `sender_province_id` | string | NO | `dim_province` |
| `sender_district_id` | string | NO | `dim_district` |
| `sender_ward_id` | string | YES | `dim_ward` |
| `pickup_facility_id` | string | NO | `dim_facility` (origin PO) |
| `receiver_province_id` | string | NO | `dim_province` |
| `receiver_district_id` | string | NO | `dim_district` |
| `receiver_ward_id` | string | YES | `dim_ward` |
| `delivery_facility_id` | string | NO | `dim_facility` (dest PO) |
| `declared_value_vnd` | int | YES | `fact_shipment` |
| `weight_gram` | int | NO | `fact_shipment` |
| `length_cm` / `width_cm` / `height_cm` | number | YES | volumetric weight calc |
| `item_category` | string | YES | `fact_shipment` |
| `is_fragile` | bool | NO | `fact_shipment` |
| `cod_amount_vnd` | int | NO (default 0) | `fact_shipment` |
| `shipping_fee_vnd` | int | NO | `fact_shipment` |
| `fuel_surcharge_vnd` | int | NO (0) | `fact_shipment` |
| `remote_area_fee_vnd` | int | NO (0) | `fact_shipment` |
| `insurance_fee_vnd` | int | NO (0) | `fact_shipment` |
| `total_fee_vnd` | int | NO | `fact_shipment` |
| `payment_by` | string | NO | `SENDER` / `RECEIVER` |
| `sla_committed_date` | string (date) | NO | `fact_shipment.sla_committed_date` |

```json
{
  "event_id": "a1b2...-uuid", "event_type": "shipment_created",
  "event_time": "2026-06-15T08:15:00+07:00", "processing_time": "2026-06-15T08:15:02+07:00",
  "schema_version": "v1.0", "shipment_id": "VTP26061500001",
  "partner_id": "SHOPEE", "service_type_id": "EXPRESS",
  "sender_province_id": "HN", "sender_district_id": "HN_HK", "sender_ward_id": "HN_HK_HT",
  "pickup_facility_id": "BC_HN_001",
  "receiver_province_id": "HCM", "receiver_district_id": "HCM_Q1", "receiver_ward_id": "HCM_Q1_BN",
  "delivery_facility_id": "BC_HCM_001",
  "declared_value_vnd": 350000, "weight_gram": 500,
  "length_cm": 20, "width_cm": 15, "height_cm": 10,
  "item_category": "ELECTRONICS", "is_fragile": true,
  "cod_amount_vnd": 350000, "shipping_fee_vnd": 35000, "fuel_surcharge_vnd": 5000,
  "remote_area_fee_vnd": 0, "insurance_fee_vnd": 3500, "total_fee_vnd": 43500,
  "payment_by": "SENDER", "sla_committed_date": "2026-06-17"
}
```

### 2.2 `pickup_assigned`

| Field | Type | Null | Meaning |
|---|---|---|---|
| `pickup_facility_id` | string | NO | Origin PO assigning the pickup |
| `assigned_shipper_id` | string | NO | `dim_shipper` — who will pick up |

### 2.3 `picked_up`

| Field | Type | Null | Meaning |
|---|---|---|---|
| `pickup_facility_id` | string | NO | Origin PO |
| `shipper_id` | string | NO | Shipper who picked up |
| `reweighed_gram` | int | YES | Re-measured weight; if it differs → may trigger `fee_adjusted` |

### 2.4 `return_initiated`

| Field | Type | Null | Meaning |
|---|---|---|---|
| `facility_id` | string | NO | Where return decision was made |
| `reason_code` | string | NO | `MAX_ATTEMPTS_REACHED` / `RECEIVER_REFUSED` / `UNDELIVERABLE` |

---

## 3. Topic `logistics.tracking.events` — Checkpoint scans

- **Partition key:** `facility_id` (the scanning post office / hub).
- **Partitions:** 64 · **Rate:** 500–2,000 msg/s
- **Feeds:** `fact_shipment_route` (one row/event), `fact_delivery_attempt` (delivered/failed_delivery), `fact_shipment` status timestamps.

> ⚠️ **Partition-strategy note (experiment axis).** Keying by `facility_id` optimizes per-facility KPIs (volume, throughput, congestion) and spreads load to hot facilities, **but a single shipment scatters across partitions** (it visits many facilities) → per-shipment ordering is **not** guaranteed here. Stateful per-shipment logic (dwell time, journey reconstruction) must repartition by `shipment_id` in Spark and rely on `sequence_no` + `event_time` + watermark. Comparing `facility_id` vs `shipment_id` keying is one of the project's partition-strategy experiments.

### 3.1 Common tracking fields

| Field | Type | Null | Meaning |
|---|---|---|---|
| `facility_id` | string | NO | Post office / hub where the scan happened |
| `facility_type` | string | NO | `POST_OFFICE` / `HUB` / `SORTING_CENTER` (denormalized for fast filtering) |
| `sequence_no` | int | NO | Monotonic scan order within a shipment (1,2,3…) |

### 3.2 Per-event extra fields

| `event_type` | Extra fields | → Fact |
|---|---|---|
| `arrived_at_origin_post_office` | — | `fact_shipment.origin_post_office_arrived_at` |
| `departed_origin_post_office` | `route_id` (next linehaul) | `fact_shipment.origin_post_office_departed_at` |
| `arrived_at_hub` | `route_id` (linehaul arrived on) | `fact_shipment_route` (transit in) |
| `sorted_at_hub` | — | journey step |
| `departed_hub` | `route_id` (next linehaul), `destination_facility_id` | transit out; dwell = departed−arrived |
| `arrived_at_destination_post_office` | — | `fact_shipment.destination_post_office_arrived_at` |
| `dispatched_for_delivery` | `shipper_id`, `attempt_no` | `fact_shipment.out_for_delivery_at` |
| `delivered` | `shipper_id`, `attempt_no`, `recipient_relation` | `fact_delivery_attempt` (result=SUCCESS), `fact_shipment.delivered_at` |
| `failed_delivery` | `shipper_id`, `attempt_no`, `failure_reason_code`, `failure_reason_detail` | `fact_delivery_attempt` (result=FAILED) |
| `returned_to_sender` | `facility_id` (return point) | `fact_shipment.returned_at` |

**`failure_reason_code` enum:** `RECEIVER_ABSENT` / `WRONG_ADDRESS` / `RECEIVER_REFUSED` / `PHONE_NOT_ANSWERED` / `ADDRESS_NOT_FOUND` / `OUTSIDE_DELIVERY_HOURS` / `DAMAGED_PACKAGE` / `SECURITY_ACCESS_DENIED` / `WEATHER_CONDITIONS` / `OTHER`

```json
{
  "event_id": "c3d4...-uuid", "event_type": "departed_hub",
  "event_time": "2026-06-15T22:00:00+07:00", "processing_time": "2026-06-15T22:00:03+07:00",
  "schema_version": "v1.0", "shipment_id": "VTP26061500001",
  "facility_id": "HUB_HN_CENTRAL", "facility_type": "HUB", "sequence_no": 5,
  "route_id": "LH_HN_HCM_01", "destination_facility_id": "HUB_HCM_CENTRAL"
}
```

```json
{
  "event_id": "e5f6...-uuid", "event_type": "failed_delivery",
  "event_time": "2026-06-17T10:15:00+07:00", "processing_time": "2026-06-17T14:00:00+07:00",
  "schema_version": "v1.0", "shipment_id": "VTP26061500002",
  "facility_id": "BC_HCM_001", "facility_type": "POST_OFFICE", "sequence_no": 9,
  "shipper_id": "SHP_HCM_00234", "attempt_no": 1,
  "failure_reason_code": "RECEIVER_ABSENT",
  "failure_reason_detail": "Gọi điện không nghe máy, nhà không có người"
}
```

---

## 4. Topic `logistics.financial.events` — Revenue & COD

- **Partition key:** `facility_id` (hash) · **Partitions:** 16 · **Rate:** 50–150 msg/s
- **Feeds:** `fact_financial_transaction`, and `fact_delivery_attempt.cod_collected_vnd` (join on `shipment_id` + `attempt_no`).

### 4.1 Common financial fields

| Field | Type | Null | Meaning |
|---|---|---|---|
| `facility_id` | string | NO | PO/hub recording the transaction |
| `branch_id` | string | NO | Denormalized for fast branch-level revenue rollups |
| `partner_id` | string | NO | Channel |
| `service_type_id` | string | NO | Service |

### 4.2 Per-event extra fields

| `event_type` | Extra fields | Meaning |
|---|---|---|
| `shipping_fee_confirmed` | `revenue_amount_vnd`, `shipping_fee_vnd`, `fuel_surcharge_vnd`, `remote_area_fee_vnd`, `insurance_fee_vnd` | Fee confirmed at order intake. `revenue_type=SHIPPING_FEE`. |
| `cod_collected` | `shipper_id`, `attempt_no`, `cod_amount_vnd` | COD successfully collected on delivery. |
| `cod_failed` | `shipper_id`, `attempt_no`, `cod_amount_vnd`, `reason_code` | Delivery failed → COD not collected. |
| `fee_adjusted` | `old_total_fee_vnd`, `new_total_fee_vnd`, `adjustment_amount_vnd`, `reason_code` | Reweigh / surcharge correction. `reason_code`: `REWEIGH` / `REROUTE` / `MANUAL`. |

```json
{
  "event_id": "g7h8...-uuid", "event_type": "cod_collected",
  "event_time": "2026-06-17T11:00:00+07:00", "processing_time": "2026-06-17T11:00:05+07:00",
  "schema_version": "v1.0", "shipment_id": "VTP26061500001",
  "facility_id": "BC_HCM_001", "branch_id": "CN_HCM_01",
  "partner_id": "SHOPEE", "service_type_id": "EXPRESS",
  "shipper_id": "SHP_HCM_00234", "attempt_no": 1, "cod_amount_vnd": 350000
}
```

---

## 5. Event → Fact mapping & enum normalization (Silver/Gold)

| Wire `event_type` (snake_case) | Topic | Normalized enum (`fact_shipment_route.event_type`) | Drives |
|---|---|---|---|
| `shipment_created` | shipment | `ORDER_CREATED` | `fact_shipment` insert; `fact_shipment_route` seq 1 |
| `pickup_assigned` | shipment | `PICKUP_ASSIGNED` | status |
| `picked_up` | shipment | `PICKED_UP` | `fact_shipment.pickup_at` |
| `arrived_at_origin_post_office` | tracking | `ARRIVED_AT_ORIGIN_POST_OFFICE` | route |
| `departed_origin_post_office` | tracking | `DEPARTED_ORIGIN_POST_OFFICE` | route |
| `arrived_at_hub` | tracking | `ARRIVED_AT_HUB` | route; dwell start |
| `sorted_at_hub` | tracking | `SORTED_AT_HUB` | route |
| `departed_hub` | tracking | `DEPARTED_HUB` | route; dwell end / transit start |
| `arrived_at_destination_post_office` | tracking | `ARRIVED_AT_DESTINATION_POST_OFFICE` | route |
| `dispatched_for_delivery` | tracking | `DISPATCHED_FOR_DELIVERY` | `out_for_delivery_at` |
| `delivered` | tracking | `DELIVERED` | `fact_delivery_attempt` (SUCCESS), `delivered_at` |
| `failed_delivery` | tracking | `FAILED_DELIVERY` | `fact_delivery_attempt` (FAILED) |
| `return_initiated` | shipment | `RETURN_INITIATED` | status |
| `returned_to_sender` | tracking | `RETURNED_TO_SENDER` | `returned_at` |
| `shipping_fee_confirmed` | financial | — | `fact_financial_transaction` |
| `cod_collected` | financial | — | `fact_financial_transaction`, attempt COD |
| `cod_failed` | financial | — | `fact_financial_transaction` |
| `fee_adjusted` | financial | — | `fact_financial_transaction` |

---

## 6. Watermark per topic (matches `schema_design.md` §C3)

| Topic | Watermark | Reason |
|---|---|---|
| `logistics.shipment.events` | 30–60 min | Lifecycle events rarely very late |
| `logistics.tracking.events` | 4 hours | Shippers in no-signal areas sync late |
| `logistics.financial.events` | 2 hours | COD sync delays; dedup on `event_id` |
