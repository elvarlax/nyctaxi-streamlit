#!/usr/bin/env python3
import streamlit as st
import pandas as pd
import altair as alt
import duckdb
import numpy as np
from pathlib import Path

# ----------------- App & Theme -----------------
st.set_page_config(page_title="NYC Taxi — Portfolio Dashboard", layout="wide")
alt.data_transformers.disable_max_rows()

ALT_LINE_HEIGHT = 360
ALT_LINE_STROKE = 2.4
BAR_HEIGHT_PER_ROW = 26

# Service colors (only where service comparisons add value)
SERVICE_COLOR_SCALE = alt.Scale(
    domain=["yellow", "green", "fhv", "fhvhv"],
    range=["#F4D35E", "#6DBE45", "#4A90E2", "#FF6B6B"]  # softer yellow/green/blue
)

# Neutral color for simple bars (clean & readable)
NEUTRAL_BAR_COLOR = "#6C8EBF"  # soft steel blue

# ----------------- Data source (DuckDB) -----------------
DB_PATH = (Path(__file__).parent / "nyctaxi.duckdb").resolve()
if not DB_PATH.exists():
    st.error(f"Missing DuckDB file: {DB_PATH.name}. Build it first (python build_duckdb.py).")
    st.stop()

con = duckdb.connect(str(DB_PATH), read_only=True)

# Load views into DataFrames (pre-aggregates only)
zones         = con.sql("SELECT * FROM v_dim_taxi_zone").df()
pu_zone_daily = con.sql("SELECT * FROM v_pu_zone_daily").df()                 # 02
daily_metrics = con.sql("SELECT * FROM v_daily_service_metrics").df()         # 03 & 05 & 09
zone_pairs    = con.sql("SELECT * FROM v_zone_pair_flow").df()                # 07
payment_mix   = con.sql("SELECT * FROM v_payment_mix").df()                   # 08
tip_hotspots  = con.sql("SELECT * FROM v_tip_hotspots").df()                  # 06
airport_daily = con.sql("SELECT * FROM v_airport_traffic_daily").df()         # 10
efficiency    = con.sql("SELECT * FROM v_service_efficiency").df()            # 04
rush_hour     = con.sql("SELECT * FROM v_rush_hour_pickups").df()             # 03-hourly

# ----------------- Chart helpers -----------------
def pretty_line(df, x, y, color, y_title, y_fmt=None):
    if df.empty:
        return alt.Chart(pd.DataFrame({x: [], y: []})).mark_line()
    dfx = df.copy()
    dfx[x] = pd.to_datetime(dfx[x], errors="coerce")
    y_enc = alt.Y(f"{y}:Q", title=y_title, axis=alt.Axis(format=y_fmt) if y_fmt else alt.Axis())
    return (
        alt.Chart(dfx)
        .mark_line(point=False, strokeWidth=ALT_LINE_STROKE)
        .encode(
            x=alt.X(f"{x}:T", title=None),
            y=y_enc,
            color=alt.Color(color, title="Service", scale=SERVICE_COLOR_SCALE),
            tooltip=[
                alt.Tooltip(f"{x}:T", title="Date"),
                alt.Tooltip(f"{color}:N", title="Service"),
                alt.Tooltip(f"{y}:Q", title=y_title, format=y_fmt if y_fmt else None),
            ],
        )
        .properties(height=ALT_LINE_HEIGHT)
        .interactive()
    )

def pretty_bar(df, x, y, sort='-x', x_title=None, color=None, color_scale=None, legend_title=None):
    """
    If `color` is provided, uses categorical color encoding.
    Otherwise, uses a single neutral bar color via alt.value(...)
    """
    if df.empty:
        return alt.Chart(pd.DataFrame({x: [], y: []})).mark_bar()
    height = max(260, int(len(df)) * BAR_HEIGHT_PER_ROW)

    enc = {
        "y": alt.Y(y, sort=sort, title=None),
        "x": alt.X(x, title=x_title, axis=alt.Axis(format="~s")),
        "tooltip": [y, alt.Tooltip(x, title=x_title, format="~s")],
    }

    chart = alt.Chart(df).mark_bar()

    if color:
        enc["color"] = alt.Color(color, title=legend_title or color, scale=color_scale)
    else:
        enc["color"] = alt.value(NEUTRAL_BAR_COLOR)

    return (
        chart.encode(**enc)
        .properties(height=height)
        .configure_axis(labelLimit=500, labelPadding=6)
    )

def weekly_rollup(df: pd.DataFrame, date_col: str, sum_cols: list, keep_cols: list = None):
    if df.empty:
        return df
    dfx = df.copy()
    dfx[date_col] = pd.to_datetime(dfx[date_col], errors="coerce")
    dfx["week"] = dfx[date_col].dt.to_period("W").apply(lambda p: p.start_time.normalize())
    agg = {c: "sum" for c in sum_cols}
    dims = keep_cols or [c for c in dfx.columns if c not in sum_cols + [date_col, "week"]]
    out = dfx.groupby(["week"] + dims, as_index=False).agg(agg)
    return out.rename(columns={"week": date_col})

def smooth_7d_mean(df: pd.DataFrame, date_col: str, group_cols: list, value_cols: list):
    """Exponential 7-day smoothing (doesn't drop edges like centered rolling)."""
    if df.empty:
        return df
    dfx = df.copy()
    dfx[date_col] = pd.to_datetime(dfx[date_col], errors="coerce")
    dfx = dfx.sort_values([*group_cols, date_col])
    for v in value_cols:
        dfx[v] = dfx.groupby(group_cols, dropna=False)[v].transform(lambda s: s.ewm(span=7, adjust=False).mean())
    return dfx

def filter_by_date_service(df: pd.DataFrame, start_date, end_date, services: list):
    """Filter by date range (Streamlit date objects) and optional service list."""
    if df.empty:
        return df
    out = df.copy()

    # Date filter only if pickup_date exists and has any non-null values
    if "pickup_date" in out.columns:
        pd_dates = pd.to_datetime(out["pickup_date"], errors="coerce")
        if pd_dates.notna().any():
            out["pickup_date"] = pd_dates.dt.date
            out = out[(out["pickup_date"] >= start_date) & (out["pickup_date"] <= end_date)]

    # Service filter (only if the column exists)
    if "service_type" in out.columns and services:
        out = out[out["service_type"].isin(services)]

    return out

# ----------------- Global Filters -----------------
series = []
for df in [daily_metrics, airport_daily, efficiency, rush_hour, pu_zone_daily]:
    if not df.empty and "pickup_date" in df.columns:
        s = pd.to_datetime(df["pickup_date"], errors="coerce").dropna()
        if not s.empty:
            series.append(s)

if not series:
    st.error("No dates found in nyctaxi.duckdb views. Rebuild exports for this window.")
    st.stop()

all_dates = pd.concat(series)
min_date, max_date = all_dates.min().date(), all_dates.max().date()
counts = all_dates.dt.to_period("M").value_counts()
default_period = counts.idxmax() if not counts.empty else None
default_start = default_period.start_time.date() if default_period else min_date
default_end   = min(default_period.end_time.date(), max_date) if default_period else max_date

with st.sidebar:
    st.header("Filters")
    start_date, end_date = st.date_input(
        "Date range",
        value=(default_start, default_end),
        min_value=min_date, max_value=max_date
    )
    svc_sets = []
    for df in [pu_zone_daily, daily_metrics, payment_mix, airport_daily, efficiency, rush_hour]:
        if not df.empty and "service_type" in df.columns:
            svc_sets.append(set(df["service_type"].dropna().unique().tolist()))
    all_services = sorted(set().union(*svc_sets)) if svc_sets else ["yellow", "green", "fhv", "fhvhv"]
    service_types = st.multiselect("Services", all_services, default=all_services)
    exclude_unknown = st.checkbox("Exclude Unknown zones", value=True)
    weekly_roll = st.checkbox("Weekly rollup (sums)", value=True)
    smooth = st.checkbox("7-day smoothing (averages)", value=True)

st.title("NYC Taxi — Insights Overview")

# ----------------- KPIs (from daily_metrics) -----------------
if not daily_metrics.empty:
    dm = filter_by_date_service(daily_metrics, start_date, end_date, service_types)
    kpis = pd.DataFrame({
        "trips":  [dm["trips"].sum() if "trips" in dm.columns else 0],
        "revenue":[dm["revenue"].sum() if "revenue" in dm.columns else 0],
        "tips":   [dm["tips"].sum() if "tips" in dm.columns else 0],
        "miles":  [dm["miles"].sum() if "miles" in dm.columns else 0],
        "cong":   [dm["congestion_fee"].sum() if "congestion_fee" in dm.columns else 0],
    })
else:
    kpis = pd.DataFrame({"trips":[0], "revenue":[0], "tips":[0], "miles":[0], "cong":[0]})

def fmt_currency(v):
    try:
        return f"${(v or 0):,.0f}"
    except Exception:
        return "$0"

c1, c2, c3, c4, c5, c6 = st.columns(6)
c1.metric("Trips",        int(kpis.loc[0, "trips"] or 0))
c2.metric("Revenue",      fmt_currency(kpis.loc[0, "revenue"]))
c3.metric("Tips",         fmt_currency(kpis.loc[0, "tips"]))
avg_rev_trip = (kpis.loc[0, "revenue"] / kpis.loc[0, "trips"]) if (kpis.loc[0, "trips"] or 0) > 0 else 0
rev_per_mile = (kpis.loc[0, "revenue"] / kpis.loc[0, "miles"]) if (kpis.loc[0, "miles"] or 0) > 0 else 0
c4.metric("Avg $/Trip",   f"${avg_rev_trip:.2f}")
c5.metric("$ / Mile",     f"${rev_per_mile:.2f}")
c6.metric("Congestion $", fmt_currency(kpis.loc[0, "cong"]))

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
    "8) Payment mix",
    "9) Service share of trips",
    "10) Airport traffic",
])

# 1) Trips over time — daily_service_metrics
with tabs[0]:
    st.markdown("**Question:** How did trips trend over time by service?")
    df = filter_by_date_service(daily_metrics, start_date, end_date, service_types)
    if not df.empty and {"pickup_date","service_type","trips"}.issubset(df.columns):
        plotdf = df[["pickup_date","service_type","trips"]].copy()
        if weekly_roll:
            plotdf = weekly_rollup(plotdf, "pickup_date", ["trips"])
        if smooth:
            plotdf = smooth_7d_mean(plotdf, "pickup_date", ["service_type"], ["trips"])
        st.altair_chart(
            pretty_line(plotdf, "pickup_date", "trips", "service_type", "Trips", y_fmt="~s"),
            use_container_width=True
        )
    else:
        st.info("No daily metrics available.")

# 2) Top pickup zones — pu_zone_daily (02)
with tabs[1]:
    st.markdown("**Question:** Which pickup zones had the highest trip counts?")
    df = filter_by_date_service(pu_zone_daily, start_date, end_date, service_types)
    need = {"pu_borough","pu_zone","trips"}
    if not df.empty and need.issubset(df.columns):
        if exclude_unknown:
            df = df[(df["pu_borough"] != "Unknown") & (df["pu_zone"] != "Unknown")]
        top = (df.groupby(["pu_borough","pu_zone"], as_index=False)["trips"].sum()
                 .sort_values("trips", ascending=False).head(20))
        top["zone"] = top["pu_borough"] + " — " + top["pu_zone"]
        st.altair_chart(
            pretty_bar(
                top.sort_values("trips"),
                "trips:Q", "zone:N", x_title="Trips"
            ),
            use_container_width=True
        )
    else:
        st.info("No pickup zone aggregates available.")

# 3) Hourly demand — simple colored bars
with tabs[2]:
    st.markdown("**Question:** What is the hourly demand profile?")
    df = filter_by_date_service(rush_hour, start_date, end_date, service_types)
    need = {"hour","trips"}
    if not df.empty and need.issubset(df.columns):
        hourly = df.groupby("hour", as_index=False)["trips"].sum().sort_values("hour")
        chart = (
            alt.Chart(hourly)
            .mark_bar(color=NEUTRAL_BAR_COLOR)
            .encode(
                x=alt.X("hour:O", title="Hour"),
                y=alt.Y("trips:Q", title="Trips", axis=alt.Axis(format="~s")),
                tooltip=[alt.Tooltip("hour:O", title="Hour"), alt.Tooltip("trips:Q", title="Trips", format="~s")],
            )
            .properties(height=220)
        )
        st.altair_chart(chart, use_container_width=True)
    else:
        st.info("No hourly aggregates available.")

# 4) Avg duration — selectable service (svc × borough or overall)
with tabs[3]:
    st.markdown("**Question:** Which services and boroughs have the longest trips?")

    services_available = sorted(set(efficiency["service_type"])) if "service_type" in efficiency.columns else []
    service_choices = ["All services"] + services_available
    svc_choice = st.selectbox("Service", service_choices, index=0, key="eff_svc_choice")

    svc_filter = [] if svc_choice == "All services" else [svc_choice]
    df = filter_by_date_service(efficiency, start_date, end_date, svc_filter).copy()

    need = {"pu_borough", "avg_duration_min"}
    if not df.empty and need.issubset(df.columns):
        if exclude_unknown:
            df = df[df["pu_borough"].astype(str).str.strip() != "Unknown"]
        df["avg_duration_min"] = pd.to_numeric(df["avg_duration_min"], errors="coerce")

        grp = (
            df.groupby("pu_borough", as_index=False)["avg_duration_min"]
              .mean()
              .sort_values("avg_duration_min", ascending=False)
        )

        chart = (
            alt.Chart(grp)
            .mark_bar(color=NEUTRAL_BAR_COLOR)
            .encode(
                y=alt.Y("pu_borough:N", sort="-x", title=None),
                x=alt.X("avg_duration_min:Q", title="Avg Minutes"),
                tooltip=[
                    alt.Tooltip("pu_borough:N", title="Borough"),
                    alt.Tooltip("avg_duration_min:Q", title="Avg Minutes", format=",.2f"),
                ],
            )
            .properties(height=max(260, len(grp) * BAR_HEIGHT_PER_ROW))
            .configure_axis(labelLimit=500, labelPadding=6)
        )

        if svc_choice == "All services":
            st.caption("Averaged across all services.")
        else:
            st.caption(f"Service: **{svc_choice}**")

        st.altair_chart(chart, use_container_width=True)
    else:
        st.info("No efficiency metrics available.")

# 5) Revenue per trip & mile — one metric at a time (stacked vertically)
with tabs[4]:
    st.markdown("**Question:** How does revenue per trip or per mile trend over time?")
    metric_choice = st.radio("Metric", ["Avg $/Trip", "$/Mile"], horizontal=True, key="rev_metric")

    df = filter_by_date_service(
        daily_metrics, start_date, end_date,
        [s for s in service_types if s in {"yellow","green","fhvhv"}]
    )
    need = {"pickup_date","service_type","revenue","trips","miles"}
    if not df.empty and need.issubset(df.columns):
        agg = df.groupby(["pickup_date","service_type"], as_index=False).agg(
            revenue=("revenue","sum"),
            trips=("trips","sum"),
            miles=("miles","sum"),
        )
        agg["avg_revenue_per_trip"] = agg["revenue"] / agg["trips"].replace(0, np.nan)
        agg["revenue_per_mile"]     = agg["revenue"] / agg["miles"].replace(0, np.nan)

        to_smooth = ["avg_revenue_per_trip"] if metric_choice == "Avg $/Trip" else ["revenue_per_mile"]
        if smooth:
            agg = smooth_7d_mean(agg, "pickup_date", ["service_type"], to_smooth)

        if metric_choice == "Avg $/Trip":
            plotdf = agg.rename(columns={"avg_revenue_per_trip":"value"})
            title = "Average $ per Trip"
        else:
            plotdf = agg.rename(columns={"revenue_per_mile":"value"})
            title = "$ per Mile"

        chart = (
            alt.Chart(plotdf)
            .mark_line(strokeWidth=ALT_LINE_STROKE)
            .encode(
                x=alt.X("pickup_date:T", title=None),
                y=alt.Y("value:Q", title=None, axis=alt.Axis(format="$,.2f")),
                facet=alt.Facet("service_type:N", columns=1, title=None),
                color=alt.Color("service_type:N", legend=None, scale=SERVICE_COLOR_SCALE),
                tooltip=[
                    alt.Tooltip("pickup_date:T", title="Date"),
                    alt.Tooltip("service_type:N", title="Service"),
                    alt.Tooltip("value:Q", title=title, format="$,.2f"),
                ],
            )
            .properties(height=110)
            .resolve_scale(y="independent")
        )
        st.altair_chart(chart, use_container_width=True)
    else:
        st.info("No revenue/miles metrics available.")

# 6) Tip % — tip_hotspots (07)
with tabs[5]:
    st.markdown("**Question:** Which zones have the highest tip %?")

    df = filter_by_date_service(
        tip_hotspots, start_date, end_date,
        [s for s in service_types if s in {"yellow","green","fhvhv"}]
    )
    need = {"service_type","pu_borough","pu_zone","tip_pct"}
    if not df.empty and need.issubset(df.columns):
        if exclude_unknown:
            df = df[(df["pu_borough"] != "Unknown") & (df["pu_zone"] != "Unknown")]

        # Remove Newark Airport outlier
        df = df[df["pu_zone"].str.strip().str.lower() != "newark airport"]

        df = df.sort_values("tip_pct", ascending=False).head(20)
        df["svc_zone"] = df["service_type"] + " — " + df["pu_borough"] + " — " + df["pu_zone"]

        st.altair_chart(
            pretty_bar(
                df.sort_values("tip_pct"),
                "tip_pct:Q", "svc_zone:N", x_title="Tip %",
                color="service_type:N", color_scale=SERVICE_COLOR_SCALE, legend_title="Service"
            ),
            use_container_width=True
        )
    else:
        st.info("No tip hotspot data available.")

# 7) Top PU→DO pairs — Borough → Borough only (with "All services")
with tabs[6]:
    st.markdown("**Question:** What are the most common borough-to-borough trips?")

    services_available = sorted(set(zone_pairs["service_type"])) if "service_type" in zone_pairs.columns else []
    service_choices = ["All services"] + services_available
    svc_choice = st.selectbox("Service", service_choices, index=0)

    svc_filter = [] if svc_choice == "All services" else [svc_choice]
    df = filter_by_date_service(zone_pairs, start_date, end_date, svc_filter).copy()

    need = {"pu_borough","do_borough","trips"}
    if not df.empty and need.issubset(df.columns):
        if exclude_unknown:
            df = df[(df["pu_borough"] != "Unknown") & (df["do_borough"] != "Unknown")]

        df["pu_borough"] = df["pu_borough"].astype(str).str.strip().str.replace(r"\s+", " ", regex=True)
        df["do_borough"] = df["do_borough"].astype(str).str.strip().str.replace(r"\s+", " ", regex=True)

        agg = (
            df.groupby(["pu_borough","do_borough"], as_index=False)["trips"].sum()
              .sort_values("trips", ascending=False)
        )
        agg["route"] = agg["pu_borough"] + " → " + agg["do_borough"]
        top = agg.head(15)

        chart = (
            alt.Chart(top.sort_values("trips"))
            .mark_bar(color=NEUTRAL_BAR_COLOR)
            .encode(
                y=alt.Y("route:N", sort="-x", title=None),
                x=alt.X("trips:Q", title="Trips", axis=alt.Axis(format="~s")),
                tooltip=[
                    alt.Tooltip("route:N", title="Route"),
                    alt.Tooltip("trips:Q", title="Trips", format="~s"),
                ],
            )
            .properties(height=max(260, len(top) * BAR_HEIGHT_PER_ROW))
            .configure_axis(labelLimit=500, labelPadding=6)
        )
        st.altair_chart(chart, use_container_width=True)
    else:
        st.info("No borough flow data available.")

# 8) Payment mix — payment_mix (05) — wide layout
with tabs[7]:
    st.markdown("**Question:** How does the payment mix change over time?")
    df = filter_by_date_service(
        payment_mix, start_date, end_date,
        [s for s in service_types if s in {"yellow","green","fhvhv"}]
    )
    need = {"pickup_date","service_type","payment_type","share"}
    if not df.empty and need.issubset(df.columns):
        dfx = df.copy()
        dfx["pickup_date"] = pd.to_datetime(dfx["pickup_date"], errors="coerce")

        chart = (
            alt.Chart(dfx)
            .mark_area()
            .encode(
                x=alt.X("pickup_date:T", title=None),
                y=alt.Y("share:Q", stack="normalize", title="Share", axis=alt.Axis(format=".0%")),
                color=alt.Color("payment_type:N", title="Payment"),
                facet=alt.Facet("service_type:N", columns=1, title=None),
                tooltip=[
                    alt.Tooltip("pickup_date:T", title="Date"),
                    alt.Tooltip("service_type:N", title="Service"),
                    alt.Tooltip("payment_type:N", title="Payment"),
                    alt.Tooltip("share:Q", title="Share", format=".1%"),
                ],
            )
            .properties(height=120, width=1200)
        )
        st.altair_chart(chart, use_container_width=False)
    else:
        st.info("No payment mix data available.")

# 9) Service share of trips — daily_service_metrics
with tabs[8]:
    st.markdown("**Question:** How does each service's share of trips evolve over time?")
    df = filter_by_date_service(daily_metrics, start_date, end_date, service_types)
    need = {"pickup_date","service_type","trips"}
    if not df.empty and need.issubset(df.columns):
        dfx = df[["pickup_date","service_type","trips"]].copy()
        dfx["pickup_date"] = pd.to_datetime(dfx["pickup_date"], errors="coerce")
        chart = (
            alt.Chart(dfx)
            .mark_area()
            .encode(
                x=alt.X("pickup_date:T", title=None),
                y=alt.Y("trips:Q", stack="normalize", title="Share of Trips", axis=alt.Axis(format=".0%")),
                color=alt.Color("service_type:N", title="Service", scale=SERVICE_COLOR_SCALE),
                tooltip=[
                    alt.Tooltip("pickup_date:T", title="Date"),
                    alt.Tooltip("service_type:N", title="Service"),
                    alt.Tooltip("trips:Q", title="Trips", format="~s"),
                ],
            )
            .properties(height=220)
        )
        st.altair_chart(chart, use_container_width=True)
    else:
        st.info("No service share data available.")

# 10) Airport traffic — airport_traffic_daily (08)
with tabs[9]:
    st.markdown("**Question:** How does airport traffic trend over time?")
    df = filter_by_date_service(airport_daily, start_date, end_date, [s for s in service_types if s in {"yellow","green","fhvhv"}])
    need = {"pickup_date","service_type","airport_fees"}
    if not df.empty and need.issubset(df.columns):
        plotdf = df[["pickup_date","service_type","airport_fees"]].copy()
        if weekly_roll:
            plotdf = weekly_rollup(plotdf, "pickup_date", ["airport_fees"])
        if smooth:
            plotdf = smooth_7d_mean(plotdf, "pickup_date", ["service_type"], ["airport_fees"])
        st.altair_chart(
            pretty_line(plotdf, "pickup_date", "airport_fees", "service_type", "USD", y_fmt="$,.0f"),
            use_container_width=True
        )
    else:
        st.info("No airport traffic data available.")

st.caption("Data: NYC TLC (pre-aggregated) • DB: DuckDB • Visuals: Altair")