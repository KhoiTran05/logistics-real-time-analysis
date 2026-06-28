#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import urllib.parse
import urllib.request

from catalog import SEED, build_catalog


def _ch(base_url: str, user: str, password: str, sql: str, body: bytes = b"") -> str:
    """POST a statement to the ClickHouse HTTP interface (8123)."""
    url = f"{base_url}/?{urllib.parse.urlencode({'query': sql})}"
    req = urllib.request.Request(
        url, data=body, method="POST",
        headers={"X-ClickHouse-User": user, "X-ClickHouse-Key": password},
    )
    with urllib.request.urlopen(req) as resp:
        return resp.read().decode()


def _jsoneachrow(rows: list[dict]) -> bytes:
    return "\n".join(json.dumps(r, ensure_ascii=False) for r in rows).encode("utf-8")


def _build_dims(seed: int) -> list[tuple[str, str, str, list[dict]]]:
    """Return (table, columns_ddl, order_by, rows) for each roll-up dimension."""
    cat = build_catalog(seed)

    region_mapping = {r["region_id"]: r["region_name"] for r in cat["dim_region"]}
    branch_mapping = { b["branch_id"] : ( b["branch_name"], b["region_id"] ) for b in cat["dim_branch"] }

    dim_facility = [
        {
            "facility_id": f["facility_id"], "facility_name": f["facility_name"],
            "facility_type": f["facility_type"], 
            "branch_id": f["branch_id"],
            "branch_name": branch_mapping.get(f["branch_id"], ("UNKNOWN", "UNKNOWN"))[0],
            "region_id": branch_mapping.get(f["branch_id"], ("UNKNOWN", "UNKNOWN"))[1],
            "region_name": region_mapping.get(branch_mapping.get(f["branch_id"], ("UNKNOWN", "UNKNOWN"))[1], "UNKNOWN"),
            "province_id": f["province_id"], 
            "capacity_per_day": f["capacity_per_day"]
        }
        for f in cat["dim_facility"]
    ]
    dim_partner = [
        {"partner_id": p["partner_id"], "partner_name": p["partner_name"],
         "partner_type": p["partner_type"]}
        for p in cat["dim_partner"]
    ]
    dim_service_type = [
        {"service_type_id": s["service_type_id"], "service_name": s["service_name"],
         "speed_tier": s["speed_tier"]}
        for s in cat["dim_service_type"]
    ]

    return [
        ("dim_facility",
         "facility_id String, facility_name String, facility_type String, "
         "branch_id String, branch_name String, region_id String, region_name String, "
         "province_id String, capacity_per_day UInt32",
         "facility_id", dim_facility),
        ("dim_partner",
         "partner_id String, partner_name String, partner_type String",
         "partner_id", dim_partner),
        ("dim_service_type",
         "service_type_id String, service_name String, speed_tier String",
         "service_type_id", dim_service_type),
    ]


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--host", default=os.environ.get("CLICKHOUSE_HOST", "localhost"))
    ap.add_argument("--port", default=os.environ.get("CLICKHOUSE_PORT", "8123"))
    ap.add_argument("--user", default=os.environ.get("CLICKHOUSE_USER", "admin"))
    ap.add_argument("--password", default=os.environ.get("CLICKHOUSE_PASSWORD", ""))
    ap.add_argument("--database", default=os.environ.get("CLICKHOUSE_DB", "logistics"))
    ap.add_argument("--seed", type=int, default=SEED)
    args = ap.parse_args()

    base = f"http://{args.host}:{args.port}"
    db = args.database

    _ch(base, args.user, args.password, f"CREATE DATABASE IF NOT EXISTS {db}")

    print(f"Seeding ClickHouse dims into {base}/{db} (seed={args.seed}) …")
    for table, cols, order_by, rows in _build_dims(args.seed):
        fq = f"{db}.{table}"
        _ch(base, args.user, args.password,
            f"CREATE TABLE IF NOT EXISTS {fq} ({cols}) ENGINE = MergeTree ORDER BY {order_by}")
        _ch(base, args.user, args.password, f"TRUNCATE TABLE {fq}")
        _ch(base, args.user, args.password,
            f"INSERT INTO {fq} FORMAT JSONEachRow", _jsoneachrow(rows))
        print(f"  {table:18s} {len(rows):>6,d} rows")

    print("Done.")


if __name__ == "__main__":
    main()
