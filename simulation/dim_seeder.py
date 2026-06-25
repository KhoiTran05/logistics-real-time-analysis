#!/usr/bin/env python3
import argparse
import csv
import io
import os

from catalog import SEED, build_catalog


def _cell(v) -> str:
    if v is None:
        return ""
    if isinstance(v, bool):
        return "true" if v else "false"
    return str(v)


def to_csv(rows: list[dict]) -> str:
    buf = io.StringIO()
    writer = csv.writer(buf, lineterminator="\n")
    header = list(rows[0].keys())
    writer.writerow(header)
    for r in rows:
        writer.writerow([_cell(r[c]) for c in header])
    return buf.getvalue()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out-dir", default="_dim_out", help="Local output directory")
    ap.add_argument("--s3-bucket", default=os.environ.get("ARTIFACTS_BUCKET", ""),
                    help="If set, upload CSVs to this bucket")
    ap.add_argument("--s3-prefix", default="dim-seed/", help="S3 key prefix")
    ap.add_argument("--seed", type=int, default=SEED)
    args = ap.parse_args()

    print(f"Building catalog (seed={args.seed}) …")
    catalog = build_catalog(args.seed)

    os.makedirs(args.out_dir, exist_ok=True)
    s3 = None
    if args.s3_bucket:
        import boto3
        s3 = boto3.client("s3")

    for table, rows in catalog.items():
        body = to_csv(rows)
        local_path = os.path.join(args.out_dir, f"{table}.csv")
        with open(local_path, "w", encoding="utf-8", newline="") as f:
            f.write(body)
        msg = f"  {table:18s} {len(rows):>7,d} rows -> {local_path}"
        if s3:
            key = f"{args.s3_prefix}{table}/{table}.csv"
            s3.put_object(Bucket=args.s3_bucket, Key=key,
                          Body=body.encode("utf-8"),
                          ContentType="text/csv; charset=utf-8")
            msg += f"  |  s3://{args.s3_bucket}/{key}"
        print(msg)

    print("Done.")


if __name__ == "__main__":
    main()
