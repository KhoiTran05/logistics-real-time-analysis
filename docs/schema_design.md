# Data Design Schema — Realtime Logistics System

## Assumptions (Business Assumptions)

1. **Standard Waybill Flow:** Create order → Pickup → Enter sending post office warehouse → Transit (linehaul) → Enter central hub warehouse → Transit → Enter receiving post office warehouse → Delivery → Completed / Returned.
2. **Geographical Units:** 3 levels — Province/City → District → Ward/Commune. Operational zoning according to 3 regions + 63 provinces.
3. **Post Office Code (`post_office`):** Each post office belongs to a branch, each branch belongs to a region.
4. **Transit Hub:** Different from a post office — hubs only do sorting & linehaul, not direct delivery.
5. **COD (Cash on Delivery):** Collected upon delivery; if delivery fails, COD is not collected, and the goods' value is refunded (or goods returned).
6. **Committed SLA:** Calculated in working days, depends on `service_type` and route (intra-province/inter-province/remote area).
7. **Shipper (Delivery Staff):** Each shipper belongs to a post office, has a unique employee code.
8. **Streaming Events:** Each waybill scan at a checkpoint (hub/post office) generates 1 tracking event.
9. **Delivery Failure:** Maximum 3 delivery attempts before switching to return status.
10. **Partners/E-commerce Platforms:** Orders can come from shopee, lazada, tiki, tiktok_shop, or retail customers.

---

## A. Data Source Overview

| # | Source Name | Business Meaning | Type | Update Frequency | Estimated Scale |
|---|---|---|---|---|---|
| 1 | `dim_province` | Province/City dimension | Batch (dimension) | Rarely changes | 63 records |
| 2 | `dim_district` | District dimension | Batch (dimension) | Rarely changes | ~700 records |
| 3 | `dim_ward` | Ward/Commune dimension | Batch (dimension) | Rarely changes | ~11,000 records |
| 4 | `dim_region` | Operational region dimension | Batch (dimension) | Rarely changes | ~10 records |
| 5 | `dim_branch` | Branch dimension | Batch (dimension) | Rarely changes | ~100 records |
| 6 | `dim_facility` | Post office / Hub dimension | Batch (dimension) | Rarely changes | ~2,000 records |
| 7 | `dim_service_type` | Shipping service type | Batch (dimension) | Rarely changes | ~20 records |
| 8 | `dim_route` | Linehaul transit route | Batch (dimension) | Changes seasonally | ~500 records |
| 9 | `dim_partner` | Partner / E-commerce platform | Batch (dimension) | Occasionally | ~50 records |
| 10 | `dim_shipper` | Delivery staff | Batch (dimension) | Weekly | ~50,000 records |
| 11 | `dim_date` | Date dimension | Batch (dimension) | Yearly | ~10,000 records |
| 12 | `fact_shipment` | Main shipment information | Batch (fact) | Daily snapshot | ~5,000,000 records |
| 13 | `fact_shipment_route` | Shipment journey history | Batch (fact) | Daily snapshot | ~20,000,000 records |
| 14 | `fact_delivery_attempt` | Delivery attempt history | Batch (fact) | Daily snapshot | ~3,000,000 records |
| 15 | `fact_hub_inventory` | Periodic inventory snapshot | Batch (periodic snapshot) | Hourly/Daily | ~50,000 records/day |
| 16 | `fact_financial_transaction` | Revenue & COD details | Batch (fact) | Daily | ~5,000,000 records |
| 17 | `stream_tracking_event` | Kafka topic — realtime scan events | **Streaming** | ~500-2,000 events/sec | ~100M events/month |
| 18 | `stream_shipment_status_log` | Kafka topic — status change logs | **Streaming** | ~200-500 events/sec | ~50M events/month |
| 19 | `stream_cod_collection` | Kafka topic — realtime COD collection | **Streaming** | ~100 events/sec | ~10M events/month |

---

## B. Detailed Schema Design

---

### B1. `dim_province` — Province/City Dimension

**Purpose:** Level 1 geographical dimension table. Used to analyze volume/revenue by geographical region, draw heatmap dashboards.

| Column | Data Type | PK/FK | Nullable | Business Meaning |
|---|---|---|---|---|
| `province_id` | VARCHAR(10) | PK | NOT NULL | Province code according to VN administrative standard (e.g., "HN", "HCM", "DN") |
| `province_code` | VARCHAR(10) | - | NOT NULL | Province number code according to GSO (e.g., "01", "79", "48") |
| `province_name` | VARCHAR(100) | - | NOT NULL | Full province/city name |
| `province_name_short` | VARCHAR(50) | - | NOT NULL | Short name (e.g., "Hà Nội", "TP.HCM") |
| `region_id` | VARCHAR(10) | FK→dim_region | NOT NULL | Belongs to which operational region |
| `is_remote` | BOOLEAN | - | NOT NULL | Remote area — affects SLA and fees |
| `created_at` | TIMESTAMP | - | NOT NULL | Time added to the system |

**Sample record:**
```json
{
  "province_id": "HN", "province_code": "01", "province_name": "Thành phố Hà Nội",
  "province_name_short": "Hà Nội", "region_id": "MIEN_BAC", "is_remote": false,
  "created_at": "2020-01-01T00:00:00"
}
```

---

### B2. `dim_district` — District Dimension

**Purpose:** Analyze delivery by district. Serves route assignment and SLA calculation.

| Column | Data Type | PK/FK | Nullable | Business Meaning |
|---|---|---|---|---|
| `district_id` | VARCHAR(15) | PK | NOT NULL | District code (e.g., "HN_HK" = Hanoi, Hoan Kiem) |
| `district_code` | VARCHAR(10) | - | NOT NULL | Code according to GSO |
| `district_name` | VARCHAR(100) | - | NOT NULL | District name |
| `province_id` | VARCHAR(10) | FK→dim_province | NOT NULL | Belongs to which province/city |
| `district_type` | VARCHAR(20) | - | NOT NULL | Type: QUAN/HUYEN/THI_XA/THANH_PHO_THUOC_TINH |
| `is_inner_city` | BOOLEAN | - | NOT NULL | Inner city or suburb — affects delivery slots |
| `delivery_zone_code` | VARCHAR(20) | - | NOT NULL | Internal delivery zone code (e.g., "HN_Z01") |

**Sample record:**
```json
{
  "district_id": "HN_HK", "district_code": "001", "district_name": "Quận Hoàn Kiếm",
  "province_id": "HN", "district_type": "QUAN", "is_inner_city": true,
  "delivery_zone_code": "HN_Z01"
}
```

---

### B3. `dim_ward` — Ward/Commune Dimension

**Purpose:** Most detailed level of analysis. Serves last-mile routing and shipper assignment.

| Column | Data Type | PK/FK | Nullable | Business Meaning |
|---|---|---|---|---|
| `ward_id` | VARCHAR(20) | PK | NOT NULL | Ward/Commune code |
| `ward_code` | VARCHAR(10) | - | NOT NULL | Code according to GSO |
| `ward_name` | VARCHAR(100) | - | NOT NULL | Ward/Commune name |
| `district_id` | VARCHAR(15) | FK→dim_district | NOT NULL | Belongs to which district |
| `post_code` | VARCHAR(10) | - | NOT NULL | 6-digit postal code |
| `latitude` | DECIMAL(9,6) | - | NULL | Center latitude — used for heatmaps |
| `longitude` | DECIMAL(9,6) | - | NULL | Center longitude |

**Sample record:**
```json
{
  "ward_id": "HN_HK_HT", "ward_code": "00001", "ward_name": "Phường Hàng Trống",
  "district_id": "HN_HK", "post_code": "100000", "latitude": 21.031271,
  "longitude": 105.848466
}
```

---

### B4. `dim_region` — Operational Region Dimension

**Purpose:** Highest level business zoning. Used to aggregate KPIs at the region level, allocate linehauls.

| Column | Data Type | PK/FK | Nullable | Business Meaning |
|---|---|---|---|---|
| `region_id` | VARCHAR(20) | PK | NOT NULL | Region code (e.g., "MIEN_BAC", "MIEN_TRUNG", "MIEN_NAM") |
| `region_name` | VARCHAR(100) | - | NOT NULL | Region name |
| `region_code` | VARCHAR(10) | - | NOT NULL | Short code (e.g., "MB", "MT", "MN") |
| `headquarter_province_id` | VARCHAR(10) | FK→dim_province | NOT NULL | Province where region HQ is located |

**Sample record:**
```json
{
  "region_id": "MIEN_BAC", "region_name": "Miền Bắc", "region_code": "MB",
  "headquarter_province_id": "HN"
}
```

---

### B5. `dim_branch` — Branch Dimension

**Purpose:** Business/operational unit under a region. Each branch manages multiple post offices.

| Column | Data Type | PK/FK | Nullable | Business Meaning |
|---|---|---|---|---|
| `branch_id` | VARCHAR(20) | PK | NOT NULL | Branch code (e.g., "CN_HN_01") |
| `branch_name` | VARCHAR(150) | - | NOT NULL | Branch name |
| `branch_code` | VARCHAR(20) | - | NOT NULL | Short operational code |
| `region_id` | VARCHAR(20) | FK→dim_region | NOT NULL | Belongs to which region |
| `province_id` | VARCHAR(10) | FK→dim_province | NOT NULL | Province where branch is located |
| `branch_type` | VARCHAR(30) | - | NOT NULL | Type: PROVINCE/CITY/CLUSTER |
| `manager_name` | VARCHAR(100) | - | NULL | Branch manager name (optional) |
| `is_active` | BOOLEAN | - | NOT NULL | Is active |
| `created_at` | TIMESTAMP | - | NOT NULL | Establishment date |

**Sample record:**
```json
{
  "branch_id": "CN_HN_01", "branch_name": "Chi nhánh Hà Nội 1",
  "branch_code": "HN01", "region_id": "MIEN_BAC", "province_id": "HN",
  "branch_type": "CITY", "is_active": true, "created_at": "2018-03-15T00:00:00"
}
```

---

### B6. `dim_facility` — Facility Dimension

**Purpose:** Base operational unit. The place to receive/deliver goods, sorting, and is a checkpoint in the journey.

| Column | Data Type | PK/FK | Nullable | Business Meaning |
|---|---|---|---|---|
| `facility_id` | VARCHAR(20) | PK | NOT NULL | facility code (e.g., "BC_HN_001", "HUB_HN_CENTRAL") |
| `facility_name` | VARCHAR(200) | - | NOT NULL | facility name |
| `facility_code` | VARCHAR(20) | - | NOT NULL | Internal short code |
| `facility_type` | VARCHAR(30) | - | NOT NULL | Type: POST_OFFICE/HUB/SORTING_CENTER/PARTNER_POINT |
| `branch_id` | VARCHAR(20) | FK→dim_branch | NOT NULL | Belongs to which branch |
| `province_id` | VARCHAR(10) | FK→dim_province | NOT NULL | Province/city where post office is located |
| `district_id` | VARCHAR(15) | FK→dim_district | NOT NULL | District |
| `ward_id` | VARCHAR(20) | FK→dim_ward | NULL | Ward/commune (if any) |
| `address` | VARCHAR(300) | - | NOT NULL | Full address |
| `latitude` | DECIMAL(9,6) | - | NULL | Coordinates — used for heatmaps |
| `longitude` | DECIMAL(9,6) | - | NULL | Coordinates |
| `capacity_per_day` | INT | - | NULL | Maximum capacity (packages/day) |
| `is_active` | BOOLEAN | - | NOT NULL | Is active |
| `open_time` | TIME | - | NULL | Opening time |
| `close_time` | TIME | - | NULL | Closing time |

**Sample record:**
```json
{
  "facility_id": "BC_HN_001", "facility_name": "Bưu cục Hoàn Kiếm",
  "facility_code": "HN_HK_01", "facility_type": "POST_OFFICE",
  "branch_id": "CN_HN_01", "province_id": "HN", "district_id": "HN_HK",
  "ward_id": "HN_HK_HT", "address": "15 Đinh Lễ, Hoàn Kiếm, Hà Nội",
  "latitude": 21.0326, "longitude": 105.8516,
  "capacity_per_day": 3000, "is_active": true,
  "open_time": "07:30:00", "close_time": "21:00:00"
}
```

---

### B7. `dim_service_type` — Shipping Service Type Dimension

**Purpose:** Defines service packages. Directly affects SLA, fees, and processing priority.

| Column | Data Type | PK/FK | Nullable | Business Meaning |
|---|---|---|---|---|
| `service_type_id` | VARCHAR(20) | PK | NOT NULL | Service code (e.g., "EXPRESS", "STANDARD", "SAMEDAY") |
| `service_name` | VARCHAR(100) | - | NOT NULL | Display service name |
| `service_code` | VARCHAR(20) | - | NOT NULL | Short code printed on waybill |
| `speed_tier` | VARCHAR(20) | - | NOT NULL | SAMEDAY/NEXTDAY/EXPRESS/STANDARD/ECONOMY |
| `sla_inner_city_days` | INT | - | NOT NULL | Inner-city SLA (working days) |
| `sla_same_province_days` | INT | - | NOT NULL | Intra-province SLA |
| `sla_inter_province_days` | INT | - | NOT NULL | Inter-province SLA |
| `sla_remote_days` | INT | - | NOT NULL | Remote area SLA |
| `max_weight_kg` | DECIMAL(6,2) | - | NOT NULL | Maximum weight (kg) |
| `max_size_cm3` | BIGINT | - | NULL | Maximum volume (cm³) — volumetric weight conversion |
| `supports_cod` | BOOLEAN | - | NOT NULL | Supports COD collection or not |
| `base_price_vnd` | BIGINT | - | NOT NULL | Base price (VND) — before surcharges |
| `is_active` | BOOLEAN | - | NOT NULL | Is still offered |

**Sample record:**
```json
{
  "service_type_id": "EXPRESS", "service_name": "Chuyển phát nhanh",
  "service_code": "EXP", "speed_tier": "EXPRESS",
  "sla_inner_city_days": 1, "sla_same_province_days": 1,
  "sla_inter_province_days": 2, "sla_remote_days": 4,
  "max_weight_kg": 30.00, "max_size_cm3": 500000,
  "supports_cod": true, "base_price_vnd": 35000, "is_active": true
}
```

---

### B8. `dim_route` — Linehaul Transit Route Dimension

**Purpose:** Defines transit vehicle routes between hubs. Used to calculate transit time and detect abnormal routes.

| Column | Data Type | PK/FK | Nullable | Business Meaning |
|---|---|---|---|---|
| `route_id` | VARCHAR(30) | PK | NOT NULL | Route code (e.g., "LH_HN_HCM_01") |
| `route_name` | VARCHAR(200) | - | NOT NULL | Route name |
| `origin_hub_id` | VARCHAR(20) | FK→dim_facility | NOT NULL | Origin hub |
| `destination_hub_id` | VARCHAR(20) | FK→dim_facility | NOT NULL | Destination hub |
| `transport_mode` | VARCHAR(30) | - | NOT NULL | AIR/ROAD_TRUCK/RAIL |
| `distance_km` | DECIMAL(8,2) | - | NULL | Distance in km |
| `estimated_duration_hours` | DECIMAL(5,2) | - | NOT NULL | Standard transit time (hours) |
| `departure_times` | VARCHAR(200) | - | NULL | Departure times per day (JSON array) |
| `frequency_per_day` | INT | - | NOT NULL | Number of trips/day |
| `carrier_name` | VARCHAR(100) | - | NULL | Carrier name (if outsourced) |
| `is_active` | BOOLEAN | - | NOT NULL | Route is active |
| `effective_from` | DATE | - | NOT NULL | Date route becomes effective |
| `effective_to` | DATE | - | NULL | Date route expires (NULL = still active) |

**Sample record:**
```json
{
  "route_id": "LH_HN_HCM_01", "route_name": "Hà Nội - TP.HCM Bay",
  "origin_hub_id": "HUB_HN_CENTRAL", "destination_hub_id": "HUB_HCM_CENTRAL",
  "transport_mode": "AIR", "distance_km": 1137.5,
  "estimated_duration_hours": 2.5, "departure_times": "[\"06:00\",\"12:00\",\"20:00\"]",
  "frequency_per_day": 3, "carrier_name": "Vietnam Airlines Cargo",
  "is_active": true, "effective_from": "2023-01-01", "effective_to": null
}
```

---

### B9. `dim_partner` — Partner / E-commerce Platform Dimension

**Purpose:** Manages sending partners by channel. Serves volume and revenue analysis by sales channel.

| Column | Data Type | PK/FK | Nullable | Business Meaning |
|---|---|---|---|---|
| `partner_id` | VARCHAR(20) | PK | NOT NULL | Partner code (e.g., "SHOPEE", "LAZADA", "RETAIL") |
| `partner_name` | VARCHAR(100) | - | NOT NULL | Partner name |
| `partner_type` | VARCHAR(30) | - | NOT NULL | ECOMMERCE_PLATFORM/ENTERPRISE/RETAIL/SME |
| `contract_type` | VARCHAR(30) | - | NULL | Contract type: VOLUME/FIXED/PAY_PER_USE |
| `discount_rate` | DECIMAL(5,4) | - | NULL | Discount rate (0.0 - 1.0) |
| `api_integration` | BOOLEAN | - | NOT NULL | Has automated API integration |
| `contact_email` | VARCHAR(200) | - | NULL | Contact email |
| `is_active` | BOOLEAN | - | NOT NULL | Still partnering |

**Sample record:**
```json
{
  "partner_id": "SHOPEE", "partner_name": "Shopee Việt Nam",
  "partner_type": "ECOMMERCE_PLATFORM", "contract_type": "VOLUME",
  "discount_rate": 0.15, "api_integration": true,
  "contact_email": "logistics@shopee.com", "is_active": true
}
```

---

### B10. `dim_shipper` — Delivery Staff Dimension

**Purpose:** Shipper information. Analyzes delivery KPIs by employee, evaluates first-attempt success rate.

| Column | Data Type | PK/FK | Nullable | Business Meaning |
|---|---|---|---|---|
| `shipper_id` | VARCHAR(20) | PK | NOT NULL | Employee code (e.g., "SHP_HN_00123") |
| `shipper_code` | VARCHAR(20) | - | NOT NULL | Internal short code |
| `full_name` | VARCHAR(150) | - | NOT NULL | Full name |
| `facility_id` | VARCHAR(20) | FK→dim_facility | NOT NULL | Assigned post office |
| `vehicle_type` | VARCHAR(30) | - | NOT NULL | MOTORBIKE/BICYCLE/ON_FOOT/VAN |
| `vehicle_plate` | VARCHAR(20) | - | NULL | Vehicle license plate |
| `phone` | VARCHAR(15) | - | NOT NULL | Phone number (partially masked in logs) |
| `join_date` | DATE | - | NOT NULL | Join date |
| `is_active` | BOOLEAN | - | NOT NULL | Currently working |
| `avg_daily_capacity` | INT | - | NULL | Average deliveries/day |

**Sample record:**
```json
{
  "shipper_id": "SHP_HN_00123", "shipper_code": "HN_K123",
  "full_name": "Nguyễn Văn A", "facility_id": "BC_HN_001",
  "vehicle_type": "MOTORBIKE", "vehicle_plate": "29F1-12345",
  "phone": "0901****23", "join_date": "2021-06-01",
  "is_active": true, "avg_daily_capacity": 45
}
```

---

### B11. `dim_date` — Date Dimension

**Purpose:** Standard date dimension for time-based analysis, filtering by day of week, holidays, working days.

| Column | Data Type | PK/FK | Nullable | Business Meaning |
|---|---|---|---|---|
| `date_id` | INT | PK | NOT NULL | Format YYYYMMDD (e.g., 20240601) |
| `full_date` | DATE | - | NOT NULL | Actual date value |
| `day_of_week` | INT | - | NOT NULL | 1 (Monday) to 7 (Sunday) |
| `day_name` | VARCHAR(10) | - | NOT NULL | Monday, Tuesday... |
| `day_of_month` | INT | - | NOT NULL | 1 to 31 |
| `month_no` | INT | - | NOT NULL | 1 to 12 |
| `month_name` | VARCHAR(10) | - | NOT NULL | January, February... |
| `year_no` | INT | - | NOT NULL | Year (e.g., 2024) |
| `quarter_no` | INT | - | NOT NULL | 1 to 4 |
| `is_weekend` | BOOLEAN | - | NOT NULL | True if Saturday or Sunday |
| `is_holiday` | BOOLEAN | - | NOT NULL | True if national holiday |
| `holiday_name` | VARCHAR(100) | - | NULL | Name of the holiday if applicable |
| `is_working_day` | BOOLEAN | - | NOT NULL | True if not weekend and not holiday (used for SLA) |

**Sample record:**
```json
{
  "date_id": 20240601,
  "full_date": "2024-06-01",
  "day_of_week": 6,
  "day_name": "Saturday",
  "day_of_month": 1,
  "month_no": 6,
  "month_name": "June",
  "year_no": 2024,
  "quarter_no": 2,
  "is_weekend": true,
  "is_holiday": false,
  "holiday_name": null,
  "is_working_day": false
}
```

---

### B12. `fact_shipment` — Main Shipment Information Fact ⭐

**Purpose:** Central fact table. Contains all static information of a shipment from creation to completion. Scale: ~5 million records.

| Column | Data Type | PK/FK | Nullable | Business Meaning |
|---|---|---|---|---|
| `id` | BIGINT | PK (auto) | NOT NULL | Auto-increment ID |
| `shipment_id` | VARCHAR(30) | | NOT NULL | Source shipment ID (e.g., "VTP24060100001") |
| `partner_id` | VARCHAR(20) | FK→dim_partner | NOT NULL | Sending channel (Shopee, retail...) |
| `service_type_id` | VARCHAR(20) | FK→dim_service_type | NOT NULL | Shipping service type |
| **--- SENDER ---** | | | | |
| `sender_province_id` | VARCHAR(10) | FK→dim_province | NOT NULL | Sender's province |
| `sender_district_id` | VARCHAR(15) | FK→dim_district | NOT NULL | Sender's district |
| `sender_ward_id` | VARCHAR(20) | FK→dim_ward | NULL | Sender's ward |
| `pickup_facility_id` | VARCHAR(20) | FK→dim_facility | NOT NULL | Pickup post office |
| **--- RECEIVER ---** | | | | |
| `receiver_province_id` | VARCHAR(10) | FK→dim_province | NOT NULL | Delivery province |
| `receiver_district_id` | VARCHAR(15) | FK→dim_district | NOT NULL | Delivery district |
| `receiver_ward_id` | VARCHAR(20) | FK→dim_ward | NULL | Delivery ward |
| `delivery_facility_id` | VARCHAR(20) | FK→dim_facility | NOT NULL | Delivery post office |
| **--- GOODS ---** | | | | |
| `declared_value_vnd` | BIGINT | - | NULL | Declared value of goods (VND) |
| `weight_gram` | INT | - | NOT NULL | Actual weight (gram) |
| `length_cm` | DECIMAL(6,1) | - | NULL | Package length |
| `width_cm` | DECIMAL(6,1) | - | NULL | Package width |
| `height_cm` | DECIMAL(6,1) | - | NULL | Package height |
| `item_category` | VARCHAR(50) | - | NULL | Item category |
| `is_fragile` | BOOLEAN | - | NOT NULL | Fragile item  |
| **--- COD & FEES ---** | | | | |
| `cod_amount_vnd` | BIGINT | - | NOT NULL DEFAULT 0 | COD amount to collect (0 if no COD) |
| `shipping_fee_vnd` | BIGINT | - | NOT NULL DEFAULT 0| Total shipping fee charged to customer |
| `fuel_surcharge_vnd` | BIGINT | - | NOT NULL DEFAULT 0 | Fuel surcharge |
| `remote_area_fee_vnd` | BIGINT | - | NOT NULL DEFAULT 0 | Remote area surcharge |
| `insurance_fee_vnd` | BIGINT | - | NOT NULL DEFAULT 0 | Goods insurance fee |
| `total_fee_vnd` | BIGINT | - | NOT NULL DEFAULT 0| Total fee (shipping + surcharges) |
| **--- SLA ---** | | | | |
| `sla_committed_date` | DATE | - | NOT NULL | Committed delivery date (SLA deadline) |
| **--- STATUS ---** | | | | |
| `current_status` | VARCHAR(30) | - | NOT NULL | Current status (see below) |
| `current_facility_id` | VARCHAR(20) | FK→dim_facility | NULL | Currently at which post office/hub |
| `assigned_shipper_id` | VARCHAR(20) | FK→dim_shipper | NULL | Shipper assigned for delivery |
| **--- TIMESTAMPS ---** | | | | |
| `created_at` | TIMESTAMP | - | NOT NULL | Time of order creation |
| `pickup_at` | TIMESTAMP | - | NULL | Time of successful pickup |
| `origin_post_office_arrived_at` | TIMESTAMP | - | NULL | Time arrived at origin post office | 
| `origin_post_office_departed_at` | TIMESTAMP | - | NULL | Time departed from origin post office | 
| `first_hub_arrived_at` | TIMESTAMP | - | NULL | Time arrived at first hub |
| `last_hub_departed_at` | TIMESTAMP | - | NULL | Time departed from last hub |
| `destination_post_office_arrived_at` | TIMESTAMP | - | NULL | Time arrived at destination post office |
| `out_for_delivery_at` | TIMESTAMP | - | NULL | Time started out for delivery |
| `delivered_at` | TIMESTAMP | - | NULL | Time of successful delivery |
| `returned_at` | TIMESTAMP | - | NULL | Time of return |
| `updated_at` | TIMESTAMP | - | NOT NULL | Last update time |
| **--- FLAGS ---** | | | | |
| `is_delayed` | BOOLEAN | - | NOT NULL DEFAULT false | Has breached SLA |
| `is_returned` | BOOLEAN | - | NOT NULL DEFAULT false | Is returning |
| `is_lost` | BOOLEAN | - | NOT NULL DEFAULT false | Lost item |

**Current_status enum:**
`CREATED` → `PICKUP_ASSIGNED` → `PICKED_UP` → `AT_ORIGIN_HUB` → `IN_TRANSIT` → `AT_DESTINATION_HUB` → `OUT_FOR_DELIVERY` → `DELIVERED` / `FAILED_DELIVERY` / `RETURNING` / `RETURNED` / `LOST`

**Sample record:**
```json
{
  "id": 1,
  "shipment_id": "VTP24060100001", 
  "partner_id": "SHOPEE",
  "service_type_id": "EXPRESS",
  "sender_province_id": "HN", "sender_district_id": "HN_HK",
  "sender_ward_id": "HN_HK_HT",
  "pickup_facility_id": "BC_HN_001",
  "receiver_province_id": "HCM", "receiver_district_id": "HCM_Q1",
  "receiver_ward_id": "HCM_Q1_BN",
  "delivery_facility_id": "BC_HCM_001",
  "declared_value_vnd": 350000, "actual_weight_gram": 500,
  "volumetric_weight_gram": 400, "charged_weight_gram": 500,
  "cod_amount_vnd": 350000, "shipping_fee_vnd": 35000,
  "fuel_surcharge_vnd": 5000, "remote_area_fee_vnd": 0,
  "insurance_fee_vnd": 3500, "total_fee_vnd": 43500,
  "payment_by": "SENDER",
  "sla_committed_date": "2024-06-03",
  "current_status": "IN_TRANSIT",
  "current_facility_id": "HUB_HN_CENTRAL",
  "assigned_shipper_id": null,
  "created_at": "2024-06-01T08:15:00", "pickup_at": "2024-06-01T10:30:00",
  "origin_post_office_arrived_at": "2024-06-01T12:00:00",
  "origin_post_office_departed_at": "2024-06-01T13:00:00",
  "first_hub_arrived_at": "2024-06-01T14:00:00",
  "last_hub_departed_at": null, 
  "destination_post_office_arrived_at": null,
  "out_for_delivery_at": null, 
  "delivered_at": null, "returned_at": null,
  "updated_at": "2024-06-01T20:00:00",
  "is_delayed": false, "is_returned": false, "is_lost": false
}
```

---

### B13. `fact_shipment_route` — Shipment Journey Fact ⭐

**Purpose:** History of each checkpoint the shipment passes through. This is the largest table — ~4 records/order on average. Scale: ~20 million records.

| Column | Data Type | PK/FK | Nullable | Business Meaning |
|---|---|---|---|---|
| `id` | BIGINT | PK (auto) | NOT NULL | Auto-increment ID |
| `shipment_id` | VARCHAR(30) | | NOT NULL | Source shipment ID (e.g., "VTP24060100001") |
| `date_id` | INT | FK→dim_date | NOT NULL | Date of the event |
| `facility_id` | VARCHAR(20) | FK→dim_facility | NOT NULL | Post office/Hub where event occurred |
| `route_id` | VARCHAR(30) | FK→dim_route | NULL | Linehaul route (if it's a transit event) |
| `shipper_id` | VARCHAR(20) | FK→dim_shipper | NULL | Shipper assigned for delivery |
| `sequence_no` | INT | - | NOT NULL | Event order number | 
| `event_type` | VARCHAR(40) | - | NOT NULL | Event type (see enum) |
| `event_time` | TIMESTAMP | - | NOT NULL | Time event occurred (event time) |
| `status_before` | VARCHAR(30) | - | NULL | Status before event |

**Event_type enum:**
`ORDER_CREATED` / `PICKUP_ASSIGNED` / `PICKED_UP` / `ARRIVED_AT_ORIGIN_POST_OFFICE` / `DEPARTED_ORIGIN_POST_OFFICE` / `ARRIVED_AT_HUB` / `SORTED_AT_HUB` / `DEPARTED_HUB` / `ARRIVED_AT_DESTINATION_POST_OFFICE` / `DISPATCHED_FOR_DELIVERY` / `DELIVERY_ATTEMPTED` / `DELIVERED` / `FAILED_DELIVERY` / `RETURN_INITIATED` / `ARRIVED_AT_RETURN_HUB` / `RETURNED_TO_SENDER`

**Sample record:**
```json
{
  "id": 1,
  "shipment_id": "VTP24060100001",
  "date_id": "20240601",
  "facility_id": "HUB_HN_CENTRAL",
  "route_id": null,
  "shipper_id": "SHIPPER_HN_001",
  "sequence_no": 1,
  "event_type": "ARRIVED_AT_HUB",
  "event_time": "2024-06-01T14:00:00",
  "status_before": "PICKED_UP"
}
```

---

### B14. `fact_delivery_attempt` — Delivery Attempt History Fact

**Purpose:** Details of each delivery attempt. Serves `first_attempt_success_rate` calculation, failure reason analysis.

| Column | Data Type | PK/FK | Nullable | Business Meaning |
|---|---|---|---|---|
| `id` | BIGINT | PK (auto) | NOT NULL | Auto-increment ID |
| `shipment_id` | VARCHAR(30) | | NOT NULL | Source shipment ID (e.g., "VTP24060100001") |
| `attempt_no` | INT | - | NOT NULL | Attempt number (1, 2, 3) |
| `shipper_id` | VARCHAR(20) | FK→dim_shipper | NOT NULL | Executing shipper |
| `facility_id` | VARCHAR(20) | FK→dim_facility | NOT NULL | Dispatching post office |
| `date_id` | INT | FK→dim_date | NOT NULL | Date of the event |
| `attempt_time` | TIMESTAMP | - | NOT NULL | Delivery attempt time |
| `result` | VARCHAR(20) | - | NOT NULL | SUCCESS / FAILED |
| `failure_reason_code` | VARCHAR(30) | - | NULL | Failure reason code (see enum) |
| `failure_reason_detail` | VARCHAR(300) | - | NULL | Detailed reason description |
| `cod_collected_vnd` | BIGINT | - | NOT NULL DEFAULT 0 | COD amount collected (0 if failed) |


**Failure_reason_code enum:**
`RECEIVER_ABSENT` / `WRONG_ADDRESS` / `RECEIVER_REFUSED` / `PHONE_NOT_ANSWERED` / `ADDRESS_NOT_FOUND` / `OUTSIDE_DELIVERY_HOURS` / `DAMAGED_PACKAGE` / `SECURITY_ACCESS_DENIED` / `WEATHER_CONDITIONS` / `OTHER`

**Sample record:**
```json
{
  "id": 5000001,
  "shipment_id": "VTP24060100002",
  "attempt_no": 1,
  "shipper_id": "SHP_HCM_00234",
  "facility_id": "BC_HCM_001",
  "date_id": "20240603",
  "attempt_time": "2024-06-03T10:15:00",
  "result": "FAILED",
  "failure_reason_code": "RECEIVER_ABSENT",
  "failure_reason_detail": "Gọi điện không nghe máy, nhà không có người",
  "cod_collected_vnd": 0,
}
```

---

### B15. `fact_hub_inventory` — Periodic Inventory Snapshot Fact

**Purpose:** Periodic snapshot (e.g., hourly) showing the total number of packages currently in inventory at a hub/post office. Serves congestion detection and capacity planning.

| Column | Data Type | PK/FK | Nullable | Business Meaning |
|---|---|---|---|---|
| `id` | BIGINT | PK (auto) | NOT NULL | Auto-increment ID |
| `date_id` | INT | FK→dim_date | NOT NULL | Date of the snapshot (YYYYMMDD) |
| `snapshot_time` | TIMESTAMP | - | NOT NULL | Exact time of the snapshot |
| `facility_id` | VARCHAR(20) | FK→dim_facility | NOT NULL | Hub/Post office |
| `total_shipments` | INT | - | NOT NULL | Total number of shipments currently in inventory |
| `overdue_shipments` | INT | - | NOT NULL | Number of shipments that have exceeded standard dwell time |
| `capacity_utilization_pct` | DECIMAL(5,2) | - | NULL | Percentage of hub capacity used |

**Sample record:**
```json
{
  "id": 20000001,
  "date_id": 20240601,
  "snapshot_time": "2024-06-01T14:00:00",
  "facility_id": "HUB_HN_CENTRAL",
  "total_shipments": 1250,
  "overdue_shipments": 45,
  "capacity_utilization_pct": 41.67
}
```

---

### B16. `fact_financial_transaction` — Revenue & COD Details Fact

**Purpose:** Revenue and COD details. Scale: ~5 million records.

| Column | Data Type | PK/FK | Nullable | Business Meaning |
|---|---|---|---|---|
| `id` | BIGINT | PK (auto) | NOT NULL | Auto-increment ID |
| `shipment_id` | VARCHAR(30) | | NOT NULL | Source shipment ID (e.g., "VTP24060100001") |
| `transaction_time` | TIMESTAMP | - | NOT NULL | Time revenue generated |
| `facility_id` | VARCHAR(20) | FK→dim_facility | NOT NULL | Post office recording revenue |
| `branch_id` | VARCHAR(20) | FK→dim_branch | NOT NULL | Branch (denormalized for fast queries) |
| `partner_id` | VARCHAR(20) | FK→dim_partner | NOT NULL | Channel |
| `service_type_id` | VARCHAR(20) | FK→dim_service_type | NOT NULL | Service type |
| `revenue_type` | VARCHAR(30) | - | NOT NULL | SHIPPING_FEE/COD_COLLECTION_FEE/INSURANCE/SURCHARGE |
| `revenue_amount_vnd` | BIGINT | - | NOT NULL | Amount (VND) |
| `cod_amount_vnd` | BIGINT | - | NOT NULL DEFAULT 0 | COD collected amount |
| `collected_at` | TIMESTAMP | - | NULL | Time of collection |

**Sample record:**
```json
{
  "id": 5000001,
  "shipment_id": "VTP24060100001",
  "transaction_time": "2024-06-01T08:15:00",
  "facility_id": "BC_HN_001",
  "branch_id": "CN_HN_01",
  "partner_id": "SHOPEE",
  "service_type_id": "EXPRESS",
  "revenue_type": "SHIPPING_FEE",
  "revenue_amount_vnd": 43500,
  "cod_amount_vnd": 350000,
  "collected_at": null,
}
```

### C3. Late Arriving Data Considerations

| Scenario | Handling Strategy |
|---|---|
| Scanner offline at hub → scan is synchronized late | `is_late_arrival = true` in `fact_shipment_route`; Spark watermark 30–60 minutes |
| Shipper delivers in an area without signal → status update is late | Watermark on `stream_shipment_status_log` set to 4 hours |
| COD collected but connection lost → Kafka message arrives late | Watermark for `stream_cod_collection` set to 2 hours; use `event_id` as idempotency key |

---

## F. Estimated Scale Summary

| Table | Scale | Notes |
|---|---|---|
| Dimension tables | 63 – 50,000 rows | Broadcast join friendly |
| `fact_shipment` | ~5,000,000 rows | Batch historical 6 months |
| `fact_shipment_route` | ~20,000,000 rows | ~4 events/order on average |
| `fact_delivery_attempt` | ~3,000,000 rows | ~0.6 attempt/order average |
| `fact_hub_inventory` | ~500,000 rows/day | Hourly snapshot |
| `fact_revenue` | ~5,000,000 rows | 1:1 with fact_shipment |
| `stream_tracking_event` | ~500–2,000 msg/sec | Replay batch → streaming |
| `stream_shipment_status_log` | ~200–500 msg/sec | |
| `stream_cod_collection` | ~50–150 msg/sec | |
| **Total batch** | **~33 million records** | Sufficient for millions of requests scale |
| **Total streaming/month** | **~160 million events** | |
