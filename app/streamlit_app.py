# Imports: system, pandas, streamlit, postgres connector
import os
import pandas as pd
import streamlit as st
import psycopg2
from psycopg2.extras import RealDictCursor

# Streamlit page setup
st.set_page_config(page_title="Student Stress Monitor", layout="wide")

# Title and intro text
st.title("Student Stress Monitoring — Demo Dashboard")
st.markdown(
    "This demo reads from Postgres tables created by the ETL (`master_table`, `curated_student`). "
    "If Postgres is unavailable, you can upload a CSV and explore locally."
)

# Database connection function (reads env vars, defaults to local Postgres)
def get_conn():
    host = os.getenv("PGHOST", "localhost")
    db   = os.getenv("PGDATABASE", "postgres")
    user = os.getenv("PGUSER", "postgres")
    pwd  = os.getenv("PGPASSWORD", "")
    port = int(os.getenv("PGPORT", "5432"))
    try:
        conn = psycopg2.connect(host=host, database=db, user=user, password=pwd, port=port)
        return conn
    except Exception as e:
        st.warning(f"Could not connect to Postgres: {e}")
        return None

# Cache query to avoid reloading on each refresh
@st.cache_data(show_spinner=False)
def load_master_from_db():
    conn = get_conn()
    if not conn:
        return None
    with conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
        # quick check if table has data
        cur.execute("SELECT * FROM master_table LIMIT 1;")
        if cur.rowcount == 0:
            return pd.DataFrame()
        # fetch all rows
        cur.execute("SELECT * FROM master_table;")
        rows = cur.fetchall()
        return pd.DataFrame(rows)

# Try loading data from Postgres
data = load_master_from_db()

# Allow user to switch between Postgres source or CSV upload
src = st.radio("Data source", ["Postgres (ETL output)", "Upload CSV"], horizontal=True)

if src == "Upload CSV":
    up = st.file_uploader("Upload your master CSV", type=["csv"])
    if up:
        data = pd.read_csv(up)
else:
    if data is None:
        st.info("Switch to 'Upload CSV' to explore without Postgres.")
        data = pd.DataFrame()

# Stop execution if no data
if data is None or data.empty:
    st.stop()

# Normalize column names
data.columns = [c.strip().lower().replace(" ", "_") for c in data.columns]

# Sidebar filters for categorical columns if available
col_filters = []
for cand in ["gender", "department", "class", "year", "course", "stress_bucket"]:
    if cand in data.columns:
        vals = sorted([x for x in data[cand].dropna().unique() if str(x) != ""])
        sel = st.sidebar.multiselect(f"Filter by {cand}", vals)
        if sel:
            data = data[data[cand].isin(sel)]

# Metric cards
c1, c2, c3, c4 = st.columns(4)
c1.metric("Rows", f"{len(data):,}")
num_cols = data.select_dtypes(include="number").columns
if "stress_level" in data.columns:
    c2.metric("Avg. Stress", f"{data['stress_level'].mean():.2f}")
if "sleep_duration" in data.columns:
    c3.metric("Avg. Sleep", f"{data['sleep_duration'].mean():.2f}")
if "study_hours" in data.columns:
    c4.metric("Avg. Study Hrs", f"{data['study_hours'].mean():.2f}")

# Charts (render only if the columns exist)
if "stress_bucket" in data.columns:
    st.subheader("Distribution by Stress Bucket")
    st.bar_chart(data["stress_bucket"].value_counts().sort_index())

if "stress_level" in data.columns:
    st.subheader("Stress Level Histogram")
    st.histogram(data, x="stress_level", bins=30)

# Preview table
st.subheader("Data Preview")
st.dataframe(data.head(200), use_container_width=True)