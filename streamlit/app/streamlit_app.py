import streamlit as st
import duckdb
import pandas as pd
import re
import altair as alt

st.set_page_config(page_title="NYC Taxi — Streamlit", layout="wide")
DB_PATH = "streamlit/db/nyctaxi.duckdb"

# ----------------- Connect -----------------
try:
    con = duckdb.connect(DB_PATH, read_only=True)
except Exception:
    st.error(f"Could not open DuckDB at '{DB_PATH}'. Build it first with: python duckdb/build_duckdb.py")
    st.stop()

# DuckDB uses $named params (not :named). Convert on the fly so queries stay readable.
def q(sql: str, params: dict):
    sql = re.sub(r":([A-Za-z_]\w*)", r"$\1", sql)
    return con.execute(sql, params).df()

# ----------------- Altair Defaults -----------------
alt.data_transformers.disable_max_rows()
ALT_LINE_HEIGHT = 380
ALT_LINE_STROKE = 2.4
BAR_HEIGHT_PER_ROW = 26

# Fixed service color mapping
SERVICE_COLOR_SCALE = alt.Scale(
    domain=["yellow", "green", "fhv", "fhvhv"],
    range=["#FFD60A", "#2ECC71", "#1F77B4", "#FF6B6B"]  # Yellow, Green, Blue, Red
)

def pretty_line(df, x, y, color, y_title):
    """Reusable Altair line chart with consistent colors."""
    if df.empty:
        return alt.Chart(pd.DataFrame({x: [], y: []})).mark_line()
    dfx = df.copy()
    dfx[x] = pd.to_datetime(dfx[x])
    return (
        alt.Chart(dfx)
        .mark_line(point=True, strokeWidth=ALT_LINE_STROKE)
        .encode(
            x=alt.X(x, type="temporal", title=None),
            y=alt.Y(y, title=y_title),
            color=alt.Color(color, title="Service", scale=SERVICE_COLOR_SCALE),
            tooltip=[x, color, y]
        )
        .properties(height=ALT_LINE_HEIGHT)
        .interactive()
    )

def pretty_bar(df, x, y, sort='-x', x_title=None):
    """Horizontal bar with dynamic height so long labels are readable."""
    if df.empty:
        return alt.Chart(pd.DataFrame({x: [], y: []})).mark_bar()
    height = max(260, len(df) * BAR_HEIGHT_PER_ROW)
    return (
        alt.Chart(df)
        .mark_bar()
        .encode(
            y=alt.Y(y, sort=sort, title=None),
            x=alt.X(x, title=x_title),
            tooltip=[y, x]
        )
        .properties(height=height)
        .configure_axis(
            labelLimit=500,    # give labels plenty of width before truncating
            labelPadding=6
        )
    )

def maybe_weekly(df, date_col, group_cols, sum_cols):
    """Optional weekly rollup for SUM metrics only."""
    if df.empty or not weekly_rollup:
        return df
    dfx = df.copy()
    dfx[date_col] = pd.to_datetime(dfx[date_col])
    dfx["week"] = dfx[date_col].dt.to_period("W").apply(lambda p: p.start_time.normalize())
    group_cols2 = ["week"] + [c for c in group_cols if c != date_col]
    agg = {c: "sum" for c in sum_cols}
    out = dfx.groupby(group_cols2, as_index=False).agg(agg)
    return out.rename(columns={"week": date_col})

# ----------------- Global Filters -----------------
min_date, max_date = con.execute("SELECT MIN(pickup_date), MAX(pickup_date) FROM trips_agg").fetchone()
if min_date is None or max_date is None:
    st.error("No data found. Rebuild the DuckDB after downloading parquet.")
    st.stop()

# Default to July if present
july_min, july_max = con.execute("""
    SELECT MIN(pickup_date), MAX(pickup_date)
    FROM trips_agg
    WHERE EXTRACT(MONTH FROM pickup_date) = 7
""").fetchone()
default_start = july_min or min_date
default_end   = july_max or max_date

with st.sidebar:
    st.header("Filters")
    start_date, end_date = st.date_input(
        "Date range",
        value=(default_start, default_end),
        min_value=min_date, max_value=max_date
    )

    services_all = [r[0] for r in con.execute(
        "SELECT DISTINCT service_type FROM trips_agg ORDER BY 1"
    ).fetchall()] or ["yellow", "green", "fhv", "fhvhv"]

    service_types = st.multiselect("Services", services_all, default=services_all)

    exclude_unknown = st.checkbox("Exclude Unknown zones", value=True)
    weekly_rollup = st.checkbox("Weekly rollup (Trips/Fees only)", value=False)

# ----------------- KPIs -----------------
kpis = q("""
SELECT
  SUM(trips)                                        AS trips,
  SUM(revenue)                                      AS revenue,
  SUM(tips)                                         AS tips,
  SUM(revenue)/NULLIF(SUM(trips),0)                 AS avg_rev_per_trip,
  SUM(revenue)/NULLIF(SUM(miles),0)                 AS rev_per_mile,
  SUM(congestion_fee)+SUM(cbd_congestion_fee)       AS total_congestion_fees
FROM trips_agg
WHERE pickup_date BETWEEN :start_date AND :end_date
  AND service_type IN (SELECT * FROM UNNEST(:service_types))
""", {"start_date": start_date, "end_date": end_date, "service_types": service_types})

def fmt_currency(v):
    try:
        return f"${(v or 0):,.0f}"
    except Exception:
        return "$0"

c1, c2, c3, c4, c5, c6 = st.columns(6)
c1.metric("Trips",        int(kpis.loc[0, "trips"] or 0))
c2.metric("Revenue",      fmt_currency(kpis.loc[0, "revenue"]))
c3.metric("Tips",         fmt_currency(kpis.loc[0, "tips"]))
c4.metric("Avg $/Trip",   f"${(kpis.loc[0, 'avg_rev_per_trip'] or 0):.2f}")
c5.metric("$ / Mile",     f"${(kpis.loc[0, 'rev_per_mile'] or 0):.2f}")
c6.metric("Congestion $", fmt_currency(kpis.loc[0, "total_congestion_fees"]))

st.divider()

# ----------------- Tabs -----------------
tabs = st.tabs([
    "1) Trips over time",
    "2) Top pickup zones",
    "3) Hourly demand",
    "4) Avg duration (svc × borough)",
    "5) Revenue per trip & mile",
    "6) Tip %",
    "7) Top PU→DO pairs",
    "8) Congestion fees",
    "9) Airport fees",
    "10) Accessibility & shared (FHVHV)"
])

# 1) Trips over time — CHART + TABLE
with tabs[0]:
    st.markdown("**Question:** How did trips trend over time by service?")
    df = q("""
    SELECT pickup_date, service_type, SUM(trips) AS trips
    FROM trips_agg
    WHERE pickup_date BETWEEN :start_date AND :end_date
      AND service_type IN (SELECT * FROM UNNEST(:service_types))
    GROUP BY pickup_date, service_type
    ORDER BY pickup_date, service_type
    """, {"start_date": start_date, "end_date": end_date, "service_types": service_types})
    df = maybe_weekly(df, "pickup_date", ["pickup_date","service_type"], ["trips"])
    st.altair_chart(pretty_line(df, "pickup_date", "trips", "service_type", "Trips"),
                    use_container_width=True)
    st.dataframe(df, use_container_width=True)

# 2) Top pickup zones — CHART ONLY (Altair horizontal bars)
with tabs[1]:
    st.markdown("**Question:** Which pickup zones had the highest trip counts?")
    df = q(f"""
    SELECT pu_borough, pu_zone, SUM(trips) AS trips
    FROM trips_agg
    WHERE pickup_date BETWEEN :start_date AND :end_date
      AND service_type IN (SELECT * FROM UNNEST(:service_types))
      {"AND pu_borough <> 'Unknown' AND pu_zone <> 'Unknown'" if exclude_unknown else ""}
    GROUP BY pu_borough, pu_zone
    ORDER BY trips DESC
    LIMIT 20
    """, {"start_date": start_date, "end_date": end_date, "service_types": service_types})
    if not df.empty:
        df["zone"] = df["pu_borough"] + " — " + df["pu_zone"]
        chart = pretty_bar(df.sort_values("trips", ascending=True), "trips", "zone", x_title="Trips")
        st.altair_chart(chart, use_container_width=True)

# 3) Hourly demand — CHART ONLY
with tabs[2]:
    st.markdown("**Question:** What is the hourly demand profile?")
    df = q("""
    SELECT pickup_hour, SUM(trips) AS trips
    FROM trips_agg
    WHERE pickup_date BETWEEN :start_date AND :end_date
      AND service_type IN (SELECT * FROM UNNEST(:service_types))
    GROUP BY pickup_hour
    ORDER BY pickup_hour
    """, {"start_date": start_date, "end_date": end_date, "service_types": service_types})
    st.bar_chart(df.set_index("pickup_hour")["trips"])

# 4) Avg duration — CHART ONLY (Altair horizontal bars)
with tabs[3]:
    st.markdown("**Question:** Which services and boroughs have the longest trips?")
    df = q(f"""
    SELECT service_type, pu_borough, AVG(avg_duration_min) AS avg_duration_min
    FROM trips_agg
    WHERE pickup_date BETWEEN :start_date AND :end_date
      AND service_type IN (SELECT * FROM UNNEST(:service_types))
      {"AND pu_borough <> 'Unknown'" if exclude_unknown else ""}
    GROUP BY service_type, pu_borough
    ORDER BY avg_duration_min DESC
    LIMIT 20
    """, {"start_date": start_date, "end_date": end_date, "service_types": service_types})
    if not df.empty:
        df["svc_boro"] = df["service_type"] + " — " + df["pu_borough"]
        chart = pretty_bar(df.sort_values("avg_duration_min", ascending=True),
                           "avg_duration_min", "svc_boro", x_title="Avg Minutes")
        st.altair_chart(chart, use_container_width=True)

# 5) Revenue per trip & per mile — CHART + TABLE
with tabs[4]:
    st.markdown("**Question:** How does revenue per trip and per mile trend over time?")
    df = q("""
    SELECT pickup_date, service_type,
           SUM(revenue)/NULLIF(SUM(trips),0) AS avg_revenue_per_trip,
           SUM(revenue)/NULLIF(SUM(miles),0) AS revenue_per_mile
    FROM trips_agg
    WHERE pickup_date BETWEEN :start_date AND :end_date
      AND service_type IN ('yellow','green','fhvhv')
      AND service_type IN (SELECT * FROM UNNEST(:service_types))
    GROUP BY pickup_date, service_type
    ORDER BY pickup_date, service_type
    """, {"start_date": start_date, "end_date": end_date, "service_types": service_types})
    c1, c2 = st.columns(2)
    with c1:
        st.altair_chart(pretty_line(df, "pickup_date", "avg_revenue_per_trip", "service_type", "$/Trip"),
                        use_container_width=True)
    with c2:
        st.altair_chart(pretty_line(df, "pickup_date", "revenue_per_mile", "service_type", "$/Mile"),
                        use_container_width=True)
    st.dataframe(df, use_container_width=True)

# 6) Tip % — CHART ONLY (Altair horizontal bars)
with tabs[5]:
    st.markdown("**Question:** Which zones have the highest tip %?")
    df = q(f"""
    SELECT service_type, pu_borough, pu_zone,
           SUM(tips) / NULLIF(SUM(revenue),0) AS tip_pct
    FROM trips_agg
    WHERE pickup_date BETWEEN :start_date AND :end_date
      AND service_type IN ('yellow','green','fhvhv')
      AND service_type IN (SELECT * FROM UNNEST(:service_types))
      {"AND pu_borough <> 'Unknown' AND pu_zone <> 'Unknown'" if exclude_unknown else ""}
    GROUP BY service_type, pu_borough, pu_zone
    HAVING SUM(revenue) > 0
    ORDER BY tip_pct DESC
    LIMIT 20
    """, {"start_date": start_date, "end_date": end_date, "service_types": service_types})
    if not df.empty:
        df["svc_zone"] = df["service_type"] + " — " + df["pu_borough"] + " — " + df["pu_zone"]
        chart = pretty_bar(df.sort_values("tip_pct", ascending=True), "tip_pct", "svc_zone", x_title="Tip %")
        st.altair_chart(chart, use_container_width=True)

# 7) Top PU→DO pairs — CHART ONLY (Altair horizontal bars)
with tabs[6]:
    st.markdown("**Question:** What are the most common pickup→dropoff pairs?")
    df = q(f"""
    SELECT pu_borough, pu_zone, do_borough, do_zone, SUM(trips) AS trips
    FROM trips_agg
    WHERE pickup_date BETWEEN :start_date AND :end_date
      AND service_type IN (SELECT * FROM UNNEST(:service_types))
      {"AND pu_borough <> 'Unknown' AND pu_zone <> 'Unknown' AND do_borough <> 'Unknown' AND do_zone <> 'Unknown'" if exclude_unknown else ""}
    GROUP BY pu_borough, pu_zone, do_borough, do_zone
    ORDER BY trips DESC
    LIMIT 20
    """, {"start_date": start_date, "end_date": end_date, "service_types": service_types})
    if not df.empty:
        df["route"] = df["pu_borough"] + " — " + df["pu_zone"] + " → " + df["do_borough"] + " — " + df["do_zone"]
        chart = pretty_bar(df.sort_values("trips", ascending=True), "trips", "route", x_title="Trips")
        st.altair_chart(chart, use_container_width=True)

# 8) Congestion fees — CHART + TABLE
with tabs[7]:
    st.markdown("**Question:** How much was collected in congestion fees?")
    df = q("""
    SELECT pickup_date, service_type,
           SUM(congestion_fee)+SUM(cbd_congestion_fee) AS total_congestion_fees
    FROM trips_agg
    WHERE pickup_date BETWEEN :start_date AND :end_date
      AND service_type IN ('yellow','green','fhvhv')
      AND service_type IN (SELECT * FROM UNNEST(:service_types))
    GROUP BY pickup_date, service_type
    ORDER BY pickup_date, service_type
    """, {"start_date": start_date, "end_date": end_date, "service_types": service_types})
    st.altair_chart(pretty_line(df, "pickup_date", "total_congestion_fees", "service_type", "USD"),
                    use_container_width=True)
    st.dataframe(df, use_container_width=True)

# 9) Airport fees — CHART + TABLE (+note if some services absent)
with tabs[8]:
    st.markdown("**Question:** How do airport fees trend over time?")
    df = q("""
    SELECT pickup_date, service_type, SUM(airport_fee) AS airport_fees
    FROM trips_agg
    WHERE pickup_date BETWEEN :start_date AND :end_date
      AND service_type IN ('yellow','green','fhvhv')
      AND service_type IN (SELECT * FROM UNNEST(:service_types))
    GROUP BY pickup_date, service_type
    ORDER BY pickup_date, service_type
    """, {"start_date": start_date, "end_date": end_date, "service_types": service_types})
    st.altair_chart(pretty_line(df, "pickup_date", "airport_fees", "service_type", "USD"),
                    use_container_width=True)
    st.dataframe(df, use_container_width=True)
    present = set(df["service_type"].unique())
    missing = {"yellow","green","fhvhv"} - present
    if missing:
        st.caption("Note: No airport fee data for " + ", ".join(sorted(missing)) + " in the selected range.")

# 10) Accessibility & shared (FHVHV) — CHART + TABLE
with tabs[9]:
    st.markdown("**Question:** What share of trips are shared / WAV for FHVHV?")
    # Citywide DAILY weighted averages by trips
    rates = q("""
    SELECT
      pickup_date,
      SUM(shared_request_rate * trips) / NULLIF(SUM(trips), 0) AS shared_request_rate,
      SUM(shared_match_rate  * trips) / NULLIF(SUM(trips), 0) AS shared_match_rate,
      SUM(wav_request_rate   * trips) / NULLIF(SUM(trips), 0) AS wav_request_rate,
      SUM(wav_match_rate     * trips) / NULLIF(SUM(trips), 0) AS wav_match_rate
    FROM fhvhv_flags_daily
    WHERE pickup_date BETWEEN :start_date AND :end_date
    GROUP BY pickup_date
    ORDER BY pickup_date
    """, {"start_date": start_date, "end_date": end_date})
    if not rates.empty:
        plot_df = rates.melt(id_vars="pickup_date", var_name="metric", value_name="value")
        label_map = {
            "shared_request_rate": "Shared Request Rate",
            "shared_match_rate":   "Shared Match Rate",
            "wav_request_rate":    "WAV Request Rate",
            "wav_match_rate":      "WAV Match Rate",
        }
        plot_df["metric"] = plot_df["metric"].map(label_map)
        metric_scale = alt.Scale(
            domain=list(label_map.values()),
            range=["#6C5CE7", "#00B894", "#0984E3", "#E17055"]
        )
        chart = (
            alt.Chart(plot_df)
            .mark_line(point=True, strokeWidth=ALT_LINE_STROKE)
            .encode(
                x=alt.X("pickup_date:T", title=None),
                y=alt.Y("value:Q", title="Rate"),
                color=alt.Color("metric:N", title="Metric", scale=metric_scale),
                tooltip=[
                    alt.Tooltip("pickup_date:T", title="Date"),
                    alt.Tooltip("metric:N", title="Metric"),
                    alt.Tooltip("value:Q", title="Rate", format=".2%")
                ]
            )
            .properties(height=ALT_LINE_HEIGHT)
            .interactive()
        )
        st.altair_chart(chart, use_container_width=True)
    else:
        st.info("No FHVHV flags data available for the selected date range.")

    # Detail table
    df = q(f"""
    SELECT pickup_date, pu_borough, pu_zone,
           trips, shared_request_rate, shared_match_rate,
           wav_request_rate, wav_match_rate
    FROM fhvhv_flags_daily
    WHERE pickup_date BETWEEN :start_date AND :end_date
      {"AND pu_borough <> 'Unknown' AND pu_zone <> 'Unknown'" if exclude_unknown else ""}
    ORDER BY pickup_date DESC, trips DESC
    LIMIT 200
    """, {"start_date": start_date, "end_date": end_date})
    st.dataframe(df, use_container_width=True)

st.caption("Data: NYC TLC • Engine: DuckDB • Visuals: Altair (fixed service colors & readable zone labels)")