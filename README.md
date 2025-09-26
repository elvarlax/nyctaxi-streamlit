# NYC Taxi Streamlit Dashboard

Interactive analytics dashboard for NYC Taxi & FHVHV trips using **DuckDB** + **Streamlit**.

## Setup

```bash
# Clone repo & install dependencies
python -m venv .venv
source .venv/Scripts/activate  # Windows Git Bash
# or
source .venv/bin/activate      # macOS / Linux

pip install -r requirements.txt
```

## Build DuckDB

```bash
python duckdb/build_duckdb.py
```

This creates `streamlit/db/nyctaxi.duckdb` from the local parquet exports.

## Run Dashboard

```bash
streamlit run streamlit/app/streamlit_app.py
```

Open the provided local URL in your browser (e.g. http://localhost:8501).

---

Data: **NYC TLC Trip Records**  
Engine: **DuckDB** • Frontend: **Streamlit + Altair**