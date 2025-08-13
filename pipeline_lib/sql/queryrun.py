import duckdb
import os
from datetime import datetime
from pathlib import Path
import pipeline_lib.config as cfg
import pandas as pd
import sqlparse
import traceback

DATA_PARQUET_BASE_PATH = cfg.DATA_PARQUET_DIR_PATH

# --- Logger
import logging
logger = logging.getLogger(__name__)



class SafeDict(dict):
    def __missing__(self, key):
        return f"{{{key}}}"
    
def olap_query_run(query_name, base, project_id, reporting_week, target):
    base_dir = Path(__file__).parent

    if isinstance(base, str) and len(base)>0:
        base_code = base[0].upper()
    else:
        print(f"Olap generate: Invalid base code")
        return None
    
    query_map = {
            'smr-workflow':             f'smr_workflow_{base}.sql',
            'smr-rater-label':          f'smr_rater_label_{base}.sql',
            'smr-job-label':            f'smr_job_label_{base}.sql',
            'smr-error-contribution':   f'smr_error_contribution_{base}.sql',
            'dmp-job-incorrect':        f'dmp_job_incorrect_{base}.sql'
    }

    if query_name not in query_map:
        print(f"[ERROR] Query '{query_name}' doesn't exist")
        return None
    
    
    query_file = base_dir / query_map[query_name]
    with query_file.open("r") as f:
        query_sql = f.read()

    # Build path
    #week_str = reporting_week.strftime("%Y%m%d") if hasattr(reporting_week, "strftime") else str(reporting_week).replace("-", "")
    reporting_week_str = pd.to_datetime(reporting_week, errors="coerce").strftime("%Y-%m-%d")
    #print(f"\nParsed Report Week to ISO: {reporting_week_str}")
    #parquet_pattern = f"{project_id}/*/{project_id}_{reporting_week_str}_*_{base_code}_*.parquet"
    parquet_pattern = f"{project_id}/{reporting_week_str}/{project_id}_{reporting_week_str}_*_{base_code}_*.parquet"
    input_path = os.path.join(DATA_PARQUET_BASE_PATH, parquet_pattern)

    #print(f"Parquet Input Path: {input_path}")
    
    params = {
        "input_path": f"'{input_path}'",
        "project_id": f"'{project_id}'",
        "reporting_week": f"'{reporting_week_str}'",
        "target": target or '',
        "base": f"'{base}'"
    }

    # Rendered SQL
    try:
        rendered_sql = query_sql.format_map(SafeDict(params))
    except Exception as e:
        print(f"[ERROR] Failed to format SQL template: {e}")
        print(f"Query: {query_map[query_name]} - Params: {params}")
        traceback.print_exc()
        return None

    # Pretty-print with line numbers for debugging
    """
    print("=== Rendered SQL ===")
    formatted = sqlparse.format(rendered_sql, reindent=True, keyword_case="upper")
    for i, line in enumerate(formatted.splitlines(), start=1):
        print(f"{i:03d}: {line}")
    print("====================")
    """


    # Try executing
    try:
        return duckdb.query(rendered_sql).to_df()
    
    except Exception as e:
        print(f"[ERROR] Query execution failed: {e}")
        print(f"Query: {query_map[query_name]} - Params: {params}")
        traceback.print_exc()
        return None



