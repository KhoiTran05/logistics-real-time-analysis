-- ClickHouse serving-layer schema (KPI tables + Grafana views).


CREATE DATABASE IF NOT EXISTS ${database};

-- KPI TABLES

-- Operational financial KPIs 
CREATE TABLE IF NOT EXISTS ${database}.kpi_financial (
  window_start        DateTime,
  window_end          DateTime,
  facility_id         String,
  total_revenue_vnd   Int64,
  total_cod_vnd       Int64,
  cod_collected_count UInt64,
  cod_committed_count UInt64,
  ingested_at         DateTime
) ENGINE = SummingMergeTree
ORDER BY (window_start, window_end, facility_id);

-- Order volume by pickup facility 
CREATE TABLE IF NOT EXISTS ${database}.kpi_order_volume_facility (
  window_start       DateTime,
  window_end         DateTime,
  pickup_facility_id String,
  order_count        UInt64,
  ingested_at        DateTime
) ENGINE = SummingMergeTree
ORDER BY (window_start, window_end, pickup_facility_id);

-- Order volume by partner / service 
CREATE TABLE IF NOT EXISTS ${database}.kpi_order_volume_partner_service (
  window_start    DateTime,
  window_end      DateTime,
  partner_id      String,
  service_type_id String,
  order_count     UInt64,
  ingested_at     DateTime
) ENGINE = SummingMergeTree
ORDER BY (window_start, window_end, partner_id, service_type_id);

-- Facility flow: inbound/outbound for throughput & backlog,
-- plus failed / out-for-delivery counts for the failed-delivery rate.
CREATE TABLE IF NOT EXISTS ${database}.kpi_facility_flow (
  window_start           DateTime,
  window_end             DateTime,
  facility_id            String,
  inbound_count          UInt64,
  outbound_count         UInt64,
  failed_delivery_count  UInt64,
  out_for_delivery_count UInt64,
  ingested_at            DateTime
) ENGINE = SummingMergeTree
ORDER BY (window_start, window_end, facility_id);

-- Top failure reasons 
CREATE TABLE IF NOT EXISTS ${database}.kpi_failure_reason (
  window_start          DateTime,
  window_end            DateTime,
  failure_reason_code   String,
  failed_delivery_count UInt64,
  ingested_at           DateTime
) ENGINE = SummingMergeTree
ORDER BY (window_start, window_end, failure_reason_code);

-- Global return count. Return rate = returned / order volume
CREATE TABLE IF NOT EXISTS ${database}.kpi_returns (
  window_start   DateTime,
  window_end     DateTime,
  returned_count UInt64,
  ingested_at    DateTime
) ENGINE = SummingMergeTree
ORDER BY (window_start, window_end);

-- Per-shipment stateful KPIs 
CREATE TABLE IF NOT EXISTS ${database}.kpi_shipment_journey (
  shipment_id           String,
  status                String,
  current_facility      String,
  e2e_transit_s         Nullable(Float64),
  pickup_lead_s         Nullable(Float64),
  first_mile_s          Nullable(Float64),
  origin_po_dwell_s     Nullable(Float64),
  total_hub_dwell_s     Nullable(Float64),
  last_mile_s           Nullable(Float64),
  sla_committed_date    String,
  delivered_date        String,
  sla_met               Nullable(UInt8),
  sla_breach            Nullable(UInt8),
  attempt_count         Nullable(UInt8),
  first_attempt_success Nullable(UInt8),
  is_redelivery         Nullable(UInt8),
  updated_at            DateTime,
  -- server wall-clock write time (Spark omits it; CH fills the DEFAULT). updated_at
  -- is event-time, so ingested_at - updated_at = event->serving pipeline latency.
  ingested_at           DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY shipment_id;
-- upgrade path: CREATE ... IF NOT EXISTS won't add the column to an existing table.
ALTER TABLE ${database}.kpi_shipment_journey
  ADD COLUMN IF NOT EXISTS ingested_at DateTime DEFAULT now();

CREATE TABLE IF NOT EXISTS ${database}.anomaly_alerts (
  detected_at      DateTime,
  shipment_id      String,
  facility_id      String,
  anomaly_type     String,
  severity         String,
  detail           String,
  ingested_at      DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY (detected_at, anomaly_type);
ALTER TABLE ${database}.anomaly_alerts
  ADD COLUMN IF NOT EXISTS ingested_at DateTime DEFAULT now();

-- ROLL-UP DIMENSIONS 

CREATE TABLE IF NOT EXISTS ${database}.dim_region (
  region_id   String,
  region_name String
) ENGINE = MergeTree ORDER BY region_id;

CREATE TABLE IF NOT EXISTS ${database}.dim_branch (
  branch_id   String,
  branch_name String,
  region_id   String
) ENGINE = MergeTree ORDER BY branch_id;

CREATE TABLE IF NOT EXISTS ${database}.dim_facility (
  facility_id      String,
  facility_name    String,
  facility_type    String,
  branch_id        String,
  branch_name      String,
  region_id        String,
  region_name      String,
  province_id      String,
  capacity_per_day UInt32
) ENGINE = MergeTree ORDER BY facility_id;

CREATE TABLE IF NOT EXISTS ${database}.dim_partner (
  partner_id   String,
  partner_name String,
  partner_type String
) ENGINE = MergeTree ORDER BY partner_id;

CREATE TABLE IF NOT EXISTS ${database}.dim_service_type (
  service_type_id String,
  service_name    String,
  speed_tier      String
) ENGINE = MergeTree ORDER BY service_type_id;

-- GRAFANA VIEWS

-- Order volume by facility/branch/region
CREATE VIEW IF NOT EXISTS ${database}.v_order_volume_facility AS 
SELECT
  k.window_start,
  k.window_end,
  k.pickup_facility_id AS facility_id,
  d.facility_name,
  d.facility_type,
  d.branch_id,
  d.branch_name,
  d.region_id,
  d.region_name,
  SUM(k.order_count) AS order_count
FROM ${database}.kpi_order_volume_facility AS k
LEFT JOIN ${database}.dim_facility AS d
  ON k.pickup_facility_id = d.facility_id
GROUP BY 
  k.window_start, k.window_end, 
  k.pickup_facility_id, d.facility_name, d.facility_type,
  d.branch_id, d.branch_name,
  d.region_id, d.region_name;

-- Order volume by partner/service
CREATE VIEW IF NOT EXISTS ${database}.v_order_volume_partner_service AS
SELECT
  k.window_start,
  k.window_end,
  k.partner_id,
  k.service_type_id,
  s.service_name,
  p.partner_name,
  SUM(k.order_count) AS order_count
FROM ${database}.kpi_order_volume_partner_service AS k
LEFT JOIN ${database}.dim_partner AS p
  ON k.partner_id = p.partner_id
LEFT JOIN ${database}.dim_service_type AS s
  ON k.service_type_id = s.service_type_id
GROUP BY 
  k.window_start, k.window_end,
  k.partner_id, k.service_type_id,
  s.service_name,
  p.partner_name;

-- Financial KPI by facility/branch/region
CREATE VIEW IF NOT EXISTS ${database}.v_financial_facility AS
SELECT
  k.window_start,
  k.window_end,
  k.facility_id,
  d.facility_name,
  d.facility_type,
  d.branch_id,
  d.branch_name,
  d.region_id,
  d.region_name,
  SUM(k.total_revenue_vnd) AS total_revenue_vnd,
  SUM(k.total_cod_vnd) AS total_cod_vnd,
  -- keep the components so branch/region panels can recompute the rate
  SUM(k.cod_collected_count) AS cod_collected_count,
  SUM(k.cod_committed_count) AS cod_committed_count,
  COALESCE(
    toFloat64(SUM(k.cod_collected_count))
    / nullIf(SUM(k.cod_committed_count), 0),
    0.0
  ) AS cod_collection_success_rate
FROM ${database}.kpi_financial AS k
LEFT JOIN ${database}.dim_facility AS d
  ON k.facility_id = d.facility_id
GROUP BY
  k.window_start, k.window_end,
  k.facility_id, d.facility_name, d.facility_type,
  d.branch_id, d.branch_name,
  d.region_id, d.region_name;

-- Facility flow by facility/branch/region
CREATE VIEW IF NOT EXISTS ${database}.v_facility_flow AS 
SELECT
  k.window_start,
  k.window_end,
  k.facility_id,
  d.facility_name,
  d.facility_type,
  d.branch_id,
  d.branch_name,
  d.region_id,
  d.region_name,
  SUM(k.inbound_count) + SUM(k.outbound_count) AS facility_throughput,
  SUM(k.inbound_count) - SUM(k.outbound_count) AS facility_backlog,
  SUM(k.failed_delivery_count) AS failed_delivery_count,
  SUM(k.out_for_delivery_count) AS out_for_delivery_count,
  COALESCE(
    toFloat64(SUM(k.failed_delivery_count))
    / nullIf(SUM(k.out_for_delivery_count), 0),
    0.0
  ) AS failed_delivery_rate,
  COALESCE(
    (SUM(k.inbound_count) - SUM(k.outbound_count))
    / nullIf(d.capacity_per_day, 0) * 100,
    0.0
  ) AS capacity_utilization_pct
FROM ${database}.kpi_facility_flow AS k
LEFT JOIN ${database}.dim_facility AS d
  ON k.facility_id = d.facility_id
GROUP BY
  k.window_start, k.window_end,
  k.facility_id, d.facility_name, d.facility_type,
  d.branch_id, d.branch_name,
  d.region_id, d.region_name, d.capacity_per_day;

-- Failure reason
CREATE VIEW IF NOT EXISTS ${database}.v_failure_reason AS
SELECT
  k.window_start,
  k.window_end,
  k.failure_reason_code,
  SUM(k.failed_delivery_count) AS failed_delivery_count
FROM ${database}.kpi_failure_reason AS k
GROUP BY
  k.window_start, k.window_end,
  k.failure_reason_code;

-- Return count
CREATE VIEW IF NOT EXISTS ${database}.v_returns AS
SELECT
  k.window_start,
  k.window_end,
  SUM(k.returned_count) AS returned_count
FROM ${database}.kpi_returns AS k
GROUP BY
  k.window_start, k.window_end;

-- Return rate 
CREATE VIEW IF NOT EXISTS ${database}.v_return_rate AS
SELECT
  r.window_start,
  r.window_end,
  r.returned_count,
  o.order_count,
  round(100 * r.returned_count / nullIf(o.order_count, 0), 2) AS return_rate_pct
FROM ${database}.v_returns AS r
LEFT JOIN (
  SELECT window_start, window_end, SUM(order_count) AS order_count
  FROM ${database}.kpi_order_volume_facility
  GROUP BY window_start, window_end
) AS o
  ON r.window_start = o.window_start AND r.window_end = o.window_end;

------ STATEFUL KPI VIEWS ------ 

CREATE VIEW IF NOT EXISTS ${database}.v_shipment_journey AS
SELECT * FROM ${database}.kpi_shipment_journey FINAL;

-- Status distribution: IN_PROCESS / STUCK / LOST / DELIVERED / RETURNED.
CREATE VIEW IF NOT EXISTS ${database}.v_shipment_status_dist AS
SELECT status, count() AS shipment_count
FROM ${database}.v_shipment_journey
GROUP BY status;

-- In-process count by current facility
CREATE VIEW IF NOT EXISTS ${database}.v_in_process AS
SELECT
  j.current_facility AS facility_id,
  d.facility_name,
  d.branch_id,
  d.branch_name,
  d.region_id,
  d.region_name,
  count() AS in_process_count
FROM ${database}.v_shipment_journey AS j
LEFT JOIN ${database}.dim_facility AS d
  ON j.current_facility = d.facility_id
WHERE j.status = 'IN_PROCESS'
GROUP BY
  j.current_facility, d.facility_name,
  d.branch_id, d.branch_name, d.region_id, d.region_name;

-- Journey segment averages over completed shipments 
CREATE VIEW IF NOT EXISTS ${database}.v_journey_times AS
SELECT
  count() AS completed_count,
  avg(e2e_transit_s)     AS avg_e2e_transit_s,
  avg(pickup_lead_s)     AS avg_pickup_lead_s,
  avg(first_mile_s)      AS avg_first_mile_s,
  avg(origin_po_dwell_s) AS avg_origin_po_dwell_s,
  avg(total_hub_dwell_s) AS avg_total_hub_dwell_s,
  avg(last_mile_s)       AS avg_last_mile_s
FROM ${database}.v_shipment_journey
WHERE status IN ('DELIVERED', 'RETURNED');

-- SLA & quality rates
CREATE VIEW IF NOT EXISTS ${database}.v_sla_quality AS
SELECT
  countIf(status = 'DELIVERED') AS delivered_count,
  countIf(status = 'RETURNED')  AS returned_count,
  sum(sla_breach)               AS sla_breach_count,
  round(100 * sumIf(sla_met, status = 'DELIVERED')
        / nullIf(countIf(status = 'DELIVERED'), 0), 2) AS sla_compliance_pct,
  round(100 * sumIf(first_attempt_success, status = 'DELIVERED')
        / nullIf(countIf(status = 'DELIVERED'), 0), 2) AS first_attempt_success_pct,
  round(100 * sumIf(is_redelivery, status = 'DELIVERED')
        / nullIf(countIf(status = 'DELIVERED'), 0), 2) AS redelivery_pct
FROM ${database}.v_shipment_journey
WHERE status IN ('DELIVERED', 'RETURNED');

------- ANOMALY VIEWS -------

CREATE VIEW IF NOT EXISTS ${database}.v_anomaly_alerts AS
SELECT
  a.detected_at,
  a.shipment_id,
  a.facility_id,
  d.facility_name,
  d.branch_id,
  d.region_id,
  a.anomaly_type,
  a.severity,
  a.detail
FROM ${database}.anomaly_alerts AS a
LEFT JOIN ${database}.dim_facility AS d
  ON a.facility_id = d.facility_id;

-- Alert counts by type / severity
CREATE VIEW IF NOT EXISTS ${database}.v_anomaly_summary AS
SELECT anomaly_type, severity, count() AS alert_count
FROM ${database}.anomaly_alerts
GROUP BY anomaly_type, severity;


------- LATENCY / FRESHNESS -------

-- Stateful latency
CREATE VIEW IF NOT EXISTS ${database}.v_latency_shipment AS
SELECT
  shipment_id,
  status,
  updated_at,
  ingested_at,
  greatest(0, dateDiff('second', updated_at, ingested_at)) AS pipeline_latency_s
FROM ${database}.v_shipment_journey
WHERE status IN ('IN_PROCESS', 'DELIVERED', 'RETURNED');

CREATE VIEW IF NOT EXISTS ${database}.v_latency_summary AS
SELECT
  count()                                      AS sample_count,
  round(avg(pipeline_latency_s), 2)            AS avg_latency_s,
  round(quantile(0.50)(pipeline_latency_s), 2) AS p50_latency_s,
  round(quantile(0.95)(pipeline_latency_s), 2) AS p95_latency_s,
  max(pipeline_latency_s)                      AS max_latency_s,
  max(ingested_at)                             AS last_ingested_at,
  dateDiff('second', max(ingested_at), now())  AS data_age_s
FROM ${database}.v_latency_shipment;

-- Stateless latency
CREATE VIEW IF NOT EXISTS ${database}.v_latency_kpi AS
SELECT
  kpi_name,
  window_start,
  window_end,
  last_ingested_at,
  greatest(0, dateDiff('second', window_end, last_ingested_at)) AS pipeline_latency_s
FROM (
  SELECT 'kpi_financial' AS kpi_name, window_start, window_end, max(ingested_at) AS last_ingested_at
  FROM ${database}.kpi_financial GROUP BY window_start, window_end
  UNION ALL
  SELECT 'kpi_order_volume_facility', window_start, window_end, max(ingested_at)
  FROM ${database}.kpi_order_volume_facility GROUP BY window_start, window_end
  UNION ALL
  SELECT 'kpi_order_volume_partner_service', window_start, window_end, max(ingested_at)
  FROM ${database}.kpi_order_volume_partner_service GROUP BY window_start, window_end
  UNION ALL
  SELECT 'kpi_facility_flow', window_start, window_end, max(ingested_at)
  FROM ${database}.kpi_facility_flow GROUP BY window_start, window_end
  UNION ALL
  SELECT 'kpi_failure_reason', window_start, window_end, max(ingested_at)
  FROM ${database}.kpi_failure_reason GROUP BY window_start, window_end
  UNION ALL
  SELECT 'kpi_returns', window_start, window_end, max(ingested_at)
  FROM ${database}.kpi_returns GROUP BY window_start, window_end
)
WHERE window_end < now();

CREATE VIEW IF NOT EXISTS ${database}.v_latency_kpi_summary AS
SELECT
  kpi_name,
  count()                                      AS window_count,
  round(avg(pipeline_latency_s), 2)            AS avg_latency_s,
  round(quantile(0.50)(pipeline_latency_s), 2) AS p50_latency_s,
  round(quantile(0.95)(pipeline_latency_s), 2) AS p95_latency_s,
  max(pipeline_latency_s)                      AS max_latency_s,
  max(last_ingested_at)                        AS last_ingested_at,
  dateDiff('second', max(last_ingested_at), now()) AS data_age_s
FROM ${database}.v_latency_kpi
GROUP BY kpi_name;

------ STREAMING QUERY PROGRESS ------

CREATE TABLE IF NOT EXISTS ${database}.streaming_query_progress (
  event_ts                  DateTime,
  query_name                String,
  batch_id                  Int64,
  num_input_rows            Int64,
  input_rows_per_second     Float64,
  processed_rows_per_second Float64,
  batch_duration_ms         Int64,
  add_batch_ms              Int64,
  state_num_rows_total      Int64,
  state_memory_bytes        Int64,
  num_dropped_late_rows     Int64,
  ingested_at               DateTime DEFAULT now()
) ENGINE = MergeTree
ORDER BY (query_name, event_ts);

-- Per-stream rollup: throughput, batch latency, state footprint, dropped-late.
CREATE VIEW IF NOT EXISTS ${database}.v_stream_progress_summary AS
SELECT
  query_name,
  count()                                          AS batch_count,
  round(avg(processed_rows_per_second), 1)         AS avg_processed_rps,
  round(quantile(0.50)(processed_rows_per_second), 1) AS p50_processed_rps,
  round(quantile(0.95)(processed_rows_per_second), 1) AS p95_processed_rps,
  round(avg(batch_duration_ms), 0)                 AS avg_batch_ms,
  round(quantile(0.95)(batch_duration_ms), 0)      AS p95_batch_ms,
  sum(num_input_rows)                              AS total_input_rows,
  sum(num_dropped_late_rows)                       AS total_dropped_late_rows,
  max(state_num_rows_total)                        AS max_state_rows,
  max(state_memory_bytes)                          AS max_state_memory_bytes,
  max(event_ts)                                    AS last_event_ts
FROM ${database}.streaming_query_progress
GROUP BY query_name;