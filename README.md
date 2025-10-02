# NYC Taxi Streamlit Dashboard

Interactive analytics dashboard for **NYC Taxi & FHVHV trips** (Yellow, Green, FHV, High-Volume FHV) using **Streamlit + Altair**, backed by a **DuckDB** file of pre-aggregated views.

## Quick Start

### 1) Create & activate a virtual environment
```bash
# macOS / Linux
python3 -m venv .venv
source .venv/bin/activate
# Windows (PowerShell)
python -m venv .venv
.venv\Scripts\Activate.ps1
```

### 2) Install dependencies
```bash
pip install -r requirements.txt
```

### 3) Build the DuckDB file (one-time, or when refreshing data)
This creates `nyctaxi.duckdb` with the views your app reads.
```bash
python build_duckdb.py
```

### 4) Run the dashboard
```bash
streamlit run streamlit_app.py
```
Open the local URL (e.g., http://localhost:8501).

---

## Data Model

The app reads from **pre-aggregated DuckDB views**:

- `v_dim_taxi_zone`
- `v_pu_zone_daily`                      (Top pickup zones)
- `v_daily_service_metrics`              (Trips, revenue, miles, tips)
- `v_zone_pair_flow`                     (PU→DO pairs)
- `v_payment_mix`                        (Payment share)
- `v_tip_hotspots`                       (Top tip % zones)
- `v_airport_traffic_daily`              (Airport fees/traffic)
- `v_service_efficiency`                 (Avg duration, etc.)
- `v_rush_hour_pickups`                  (Hourly demand)

These are derived from **NYC TLC Trip Records** and written into `nyctaxi.duckdb` by `build_duckdb.py`.

---

## Tech Stack

- **Backend/Storage:** DuckDB (`nyctaxi.duckdb`)
- **Frontend:** Streamlit + Altair
- **Language:** Python

---

## Notes

- If `nyctaxi.duckdb` is missing, the app will prompt you to build it with:
  ```bash
  python build_duckdb.py
  ```
- To update data, rerun the build script and refresh the app.

---

**Data:** NYC TLC (Yellow, Green, FHV, FHVHV)  
**Storage:** DuckDB • **Frontend:** Streamlit + Altair