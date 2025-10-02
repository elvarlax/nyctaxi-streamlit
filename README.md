# NYC Taxi Streamlit Dashboard

Interactive dashboard exploring **NYC Taxi & FHVHV trips** (Yellow, Green, FHV, HVFHV) using **Streamlit + Altair**, backed by a pre-aggregated **DuckDB** file.

## Quick Start

### 1) Create & activate a virtual environment
```bash
# macOS / Linux
python3 -m venv .venv
source .venv/bin/activate

# Windows (Git Bash)
python -m venv .venv
source .venv/Scripts/activate

# Windows (PowerShell)
python -m venv .venv
.venv\Scripts\Activate.ps1
```

### 2) Install dependencies
```bash
pip install -r requirements.txt
```

### 3) Build the DuckDB file (one-time)
```bash
python build_duckdb.py
```

### 4) Run the app
```bash
streamlit run streamlit_app.py
```

---

## Data

The dashboard reads from `nyctaxi.duckdb`, built with `build_duckdb.py` from **NYC TLC Trip Records**.  
Views include trip metrics, zone flows, payment mix, tip % hotspots, and airport traffic.

---

## Stack

- **Data:** NYC TLC Trip Records  
- **Storage:** DuckDB  
- **Frontend:** Streamlit + Altair  
- **Language:** Python