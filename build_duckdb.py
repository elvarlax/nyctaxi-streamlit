import os, glob, duckdb

DATA_DIR = "data"
OUT_DB = "nyctaxi.duckdb"

# Each entry points to a FOLDER with *.parquet parts
FILES = [
    ("dim_taxi_zone",         "01_dim_taxi_zone/*.parquet"),
    ("pu_zone_daily",         "02_fact_trips_sample/*.parquet"),      # daily PU-zone trips
    ("daily_service_metrics", "03_daily_service_metrics/*.parquet"),
    ("zone_pair_flow",        "04_zone_pair_flow/*.parquet"),
    ("payment_mix",           "05_payment_mix/*.parquet"),
    ("vendor_share",          "06_vendor_share/*.parquet"),
    ("tip_hotspots",          "07_tip_hotspots/*.parquet"),
    ("airport_traffic_daily", "08_airport_traffic_daily/*.parquet"),
    ("service_efficiency",    "09_service_efficiency/*.parquet"),
    ("rush_hour_pickups",     "10_rush_hour_pickups/*.parquet"),
]

# Candidate columns that might contain a date we can normalize to pickup_date
DATE_CANDIDATES = [
    "pickup_date", "date", "service_date",
    "pickup_datetime", "tpep_pickup_datetime", "lpep_pickup_datetime",
    "fhv_pickup_datetime", "fhvhv_pickup_datetime",
]

def resolve_source(pattern_rel: str):
    """Return absolute glob pattern and number of matches; raise if empty."""
    pat = os.path.join(DATA_DIR, pattern_rel)
    parts = glob.glob(pat)
    if not parts:
        raise FileNotFoundError(f"No parquet files match: {pat}")
    return pat, len(parts)

def build_date_expr(cols):
    """Produce a DuckDB SQL expression that yields a DATE named pickup_date."""
    # map lowercase -> original
    lower = {c.lower(): c for c in cols}
    exprs = []
    # 1) preferred names
    for cname in DATE_CANDIDATES:
        if cname.lower() in lower:
            orig = lower[cname.lower()]
            quoted = f't."{orig}"' if orig.lower() == "date" or any(ch in orig for ch in ' -"') else f"t.{orig}"
            exprs.append(f"TRY_CAST({quoted} AS DATE)")
    # 2) generic *_date or *_datetime
    if not exprs:
        for orig in cols:
            low = orig.lower()
            if ("date" in low) or ("datetime" in low) or low in {"dt", "ds", "day"} or low.endswith("_date"):
                q = f't."{orig}"' if any(ch in orig for ch in ' -"') else f"t.{orig}"
                exprs.append(f"TRY_CAST({q} AS DATE)")
    # 3) *_date_key (YYYYMMDD)
    if not exprs:
        for orig in cols:
            low = orig.lower()
            if low == "date_key" or low.endswith("_date_key"):
                exprs.append(f"TRY_CAST(STRPTIME(CAST(t.{orig} AS VARCHAR), '%Y%m%d') AS DATE)")
    return f"COALESCE({', '.join(exprs)})" if exprs else "NULL::DATE"

def main():
    if os.path.exists(OUT_DB):
        os.remove(OUT_DB)
    con = duckdb.connect(OUT_DB)

    # 1) Create physical tables from folder globs
    for tbl, rel_glob in FILES:
        pat, nfiles = resolve_source(rel_glob)
        con.execute(
            f"CREATE OR REPLACE TABLE {tbl} AS SELECT * FROM read_parquet(?, union_by_name=true)",
            [pat],
        )
        cnt = con.execute(f"SELECT COUNT(*) FROM {tbl}").fetchone()[0]
        print(f"{tbl:<24} {cnt:>12,} rows  (parts:{nfiles})")

    # 2) Create views v_* and guarantee pickup_date
    for tbl, _ in FILES:
        cols = [row[1] for row in con.execute(f"PRAGMA table_info('{tbl}')").fetchall()]
        date_expr = build_date_expr(cols)
        has_pd = any(c.lower() == "pickup_date" for c in cols)
        if has_pd:
            # drop existing pickup_date and recompute (avoids REPLACE-before-defined error)
            con.execute(f"""
                CREATE OR REPLACE VIEW v_{tbl} AS
                SELECT t.* EXCLUDE (pickup_date), {date_expr} AS pickup_date
                FROM {tbl} AS t
            """)
            mode = "EXCLUDE+ADD"
        else:
            con.execute(f"""
                CREATE OR REPLACE VIEW v_{tbl} AS
                SELECT t.*, {date_expr} AS pickup_date
                FROM {tbl} AS t
            """)
            mode = "ADD"
        print(f"v_{tbl}: ensured pickup_date  [{mode}]")

    # 3) Best-effort indexes on base tables (ignore if column missing)
    def try_idx(sql):
        try: con.execute(sql)
        except Exception: pass

    for tbl in ["daily_service_metrics","payment_mix","airport_traffic_daily",
                "service_efficiency","rush_hour_pickups","pu_zone_daily",
                "zone_pair_flow","tip_hotspots","vendor_share"]:
        try_idx(f"CREATE INDEX IF NOT EXISTS idx_{tbl}_date ON {tbl}(pickup_date)")
        try_idx(f"CREATE INDEX IF NOT EXISTS idx_{tbl}_svc  ON {tbl}(service_type)")

    # 4) Sanity: overall date span across key views
    span = con.execute("""
        WITH all_dates AS (
          SELECT pickup_date FROM v_daily_service_metrics WHERE pickup_date IS NOT NULL
          UNION ALL SELECT pickup_date FROM v_airport_traffic_daily WHERE pickup_date IS NOT NULL
          UNION ALL SELECT pickup_date FROM v_service_efficiency    WHERE pickup_date IS NOT NULL
          UNION ALL SELECT pickup_date FROM v_rush_hour_pickups     WHERE pickup_date IS NOT NULL
          UNION ALL SELECT pickup_date FROM v_pu_zone_daily         WHERE pickup_date IS NOT NULL
        )
        SELECT MIN(pickup_date), MAX(pickup_date), COUNT(*) FROM all_dates
    """).fetchone()
    print(f"Date span: {span[0]} → {span[1]}  (rows sampled across views: {span[2]:,})")

    con.close()
    print(f"Built {OUT_DB}")

if __name__ == "__main__":
    main()
