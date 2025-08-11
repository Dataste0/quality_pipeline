# Start: streamlit run sql_client.py

import re, io
from pathlib import Path
import duckdb
import pandas as pd
import streamlit as st

st.set_page_config(page_title="Parquet SQL — Minimal", layout="wide")

# --- Connection in-memory DuckDB
if "con" not in st.session_state:
    st.session_state.con = duckdb.connect(database=":memory:")
    try:
        st.session_state.con.execute("INSTALL httpfs; LOAD httpfs;")
    except Exception:
        pass
con = st.session_state.con

# --- Helpers
def deduce_base_code(base: str) -> str:
    base = (base or "").strip()
    return base[0].upper() if base else ""

def to_posix(p: str) -> str:
    return Path(p).as_posix()

def build_input_path(base_dir: str, project_id: str, base: str, week: str | None):
    base_code = deduce_base_code(base)
    week_part = week if week else "*"          # if week missing -> '*'
    parquet_pattern = f"{project_id}/{week_part}/{project_id}_{week_part}_*_{base_code}_*.parquet"
    return to_posix(str(Path(base_dir) / parquet_pattern))

def run_sql_with_placeholder(sql_text: str, auto_limit: bool, limit_rows: int, **placeholders):
    """
    Sostituisce placeholder stile {{name}} nel testo SQL:
      - {{files}}  -> read_parquet(?)  (binding del path parquet)
      - {{goal}}   -> ?                (binding di un float)
    Puoi aggiungerne altri passando kwargs: run_sql_with_placeholder(..., foo='bar')

    Esempio SQL:
      SELECT * FROM {{files}} WHERE metric >= {{goal}}
    """
    sql = (sql_text or "").strip()
    params = []

    # Regex per catturare { identifier }
    token = re.compile(r"\{\s*([a-zA-Z0-9_]+)\s*\}")


    def sub_token(m):
        key = m.group(1).strip().lower()
        if key == "input_path":
            if "input_path" not in placeholders:
                raise ValueError("Placeholder {{input_path}} usato ma non fornito nei parametri.")
            params.append(placeholders["input_path"])
            return "read_parquet(?)"
        else:
            if key not in placeholders:
                raise ValueError(f"Placeholder {{{{{key}}}}} usato ma non fornito nei parametri.")
            params.append(placeholders[key])
            return "?"

    # Sostituisci tutti i placeholder e accumula i parametri in ordine
    sql = token.sub(sub_token, sql)

    # Auto-LIMIT solo per SELECT e se manca già
    if auto_limit and re.search(r"^\s*select\b", sql, re.IGNORECASE) and " limit " not in sql.lower():
        sql = f"{sql}\nLIMIT {int(limit_rows)}"

    res = con.execute(sql, params)
    try:
        return res.fetchdf()
    except Exception:
        return pd.DataFrame()



# =================  SETTINGS  =================
st.header("Settings")
base_dir = st.text_input("Base directory", value="c:/dashboard/Data_Transformed",
                         help="Transformed root directory")
project_id = st.text_input("Project ID", value="a01Hs00001ocUa9IAE")
base = st.selectbox(
    "Base",
    options=["audit", "multi", "halo", "rubric"],
    index=0
)
target_goal = st.number_input("Target Goal", min_value=0.0, value=0.8, step=0.05, format="%.2f")
use_week = st.checkbox("Use reporting week", value=True)
reporting_week = None
if use_week:
    week_in = st.text_input("reporting_week (YYYY-MM-DD)", value="2025-02-07")
    ts = pd.to_datetime(week_in, errors="coerce")
    reporting_week = ts.strftime("%Y-%m-%d") if pd.notna(ts) else None

input_path = build_input_path(base_dir, project_id, base, reporting_week)
st.caption("Glob resulting:")
st.code(input_path, language="text")

try:
    files_df = con.execute("SELECT * FROM glob(?)", [input_path]).fetchdf()
    files_found = len(files_df)
    st.write(f"Files found: {files_found}")

    if files_found > 0:
        # Show at most N rows to avoid exploding the UI
        max_rows_visual = 200
        shown_df = files_df.head(max_rows_visual)

        # Dynamic height (estimate): header + rows * row_height
        header_h = 56
        row_h = 28
        min_h = 120
        max_h = 420
        height = max(min_h, min(max_h, header_h + len(shown_df) * row_h))

        st.dataframe(shown_df, use_container_width=True, hide_index=True, height=height)
except Exception as e:
    st.warning(f"Glob preview not available: {e}")

# --- Show table structure and types ---
show_schema = st.checkbox("Show structure and column types", value=False)
if show_schema:
    try:
        schema_df = con.execute(
            "DESCRIBE SELECT * FROM read_parquet(?) LIMIT 0", [input_path]
        ).fetchdf()
        st.markdown("**Schema:**")
        st.dataframe(schema_df, use_container_width=True, hide_index=True)
    except Exception as e:
        st.warning(f"Unable to obtain schema: {e}")


st.divider()

# =================  SQL  =================
st.header("SQL")
st.caption("Use the placeholder **{input_path}** to point to your glob Parquet.")
example = (
    "WITH alldata AS (\n"
    "   SELECT *\n"
    "   FROM {input_path}\n"
    "   WHERE 1\n"
    ")\n"
    "\n"
    "SELECT * FROM alldata"
)
sql_text = st.text_area("Query", example, height=220)
auto_limit = st.toggle("Auto-LIMIT if missing (only for SELECT)", value=True)
limit_rows = st.number_input("LIMIT", min_value=1, value=1000, step=100)
run = st.button("Run", type="primary")

st.divider()

# =================  RESULT  =================
st.header("Result")
if run:
    try:
        df = run_sql_with_placeholder(
            sql_text,
            auto_limit,
            limit_rows,
            input_path=input_path,
            target=target_goal
        )

        # Convert only True/False to strings, leave Na/NaN unchanged
        bool_cols = df.select_dtypes(include=["bool"]).columns
        for col in bool_cols:
            df[col] = df[col].apply(lambda x: "true" if x is True else "false" if x is False else x)

        if df is not None and not df.empty:
            st.caption(f"Rows: {len(df):,}")
            st.dataframe(df, use_container_width=True, height=420)
            csv_bytes = df.to_csv(index=False).encode("utf-8")
            st.download_button("Download CSV", data=csv_bytes, file_name="result.csv", mime="text/csv")
        else:
            st.info("Query executed. No rows to display.")
    except Exception as e:
        st.error(f"Error during execution: {e}")
