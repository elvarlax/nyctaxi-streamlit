#!/usr/bin/env python3
"""
Builds a local DuckDB from the exported Parquet snapshots.

Input (local cache):
  _cache/streamlit_trips_agg/*.parquet
  _cache/streamlit_fhvhv_flags/*.parquet
  _cache/dim_taxi_zone/*.parquet

Output:
  streamlit/db/nyctaxi.duckdb
"""

import os
import duckdb

# -------- Paths (edit if you keep data elsewhere) --------
SRC_TRIPS   = "_cache/streamlit_trips_agg/*.parquet"
SRC_FLAGS   = "_cache/streamlit_fhvhv_flags/*.parquet"
SRC_DIMZONE = "_cache/dim_taxi_zone/*.parquet"

OUT_DIR = "streamlit/db"
OUT_DB  = os.path.join(OUT_DIR, "nyctaxi.duckdb")

def ensure_inputs_exist():
    missing = []
    for glob in [SRC_TRIPS, SRC_FLAGS, SRC_DIMZONE]:
        # duckdb/parquet_scan can cope with empty globs, but we want a friendly error
        import glob as pyglob
        if not pyglob.glob(glob):
            missing.append(glob)
    if missing:
        raise FileNotFoundError(
            "Missing Parquet inputs. Make sure you downloaded the exports:\n  - "
            + "\n  - ".join(missing)
        )

def main():
    os.makedirs(OUT_DIR, exist_ok=True)
    if os.path.exists(OUT_DB):
        os.remove(OUT_DB)

    ensure_inputs_exist()

    con = duckdb.connect(OUT_DB)

    # Create tables directly from Parquet (keeps schema from exports/views)
    con.execute("CREATE OR REPLACE TABLE trips_agg AS SELECT * FROM parquet_scan(?, filename=true)", [SRC_TRIPS])
    con.execute("CREATE OR REPLACE TABLE fhvhv_flags_daily AS SELECT * FROM parquet_scan(?, filename=true)", [SRC_FLAGS])
    con.execute("CREATE OR REPLACE TABLE dim_taxi_zone AS SELECT * FROM parquet_scan(?, filename=true)", [SRC_DIMZONE])

    # Helpful indices for interactive filters
    con.execute("CREATE INDEX IF NOT EXISTS idx_trips_date    ON trips_agg(pickup_date)")
    con.execute("CREATE INDEX IF NOT EXISTS idx_trips_service ON trips_agg(service_type)")

    # Row count sanity
    for tbl in ["trips_agg", "fhvhv_flags_daily", "dim_taxi_zone"]:
        cnt = con.execute(f"SELECT COUNT(*) FROM {tbl}").fetchone()[0]
        print(f"{tbl}: {cnt:,} rows")

    con.close()
    print(f"Built DuckDB at {OUT_DB}")

if __name__ == "__main__":
    main()
