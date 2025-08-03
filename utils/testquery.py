import duckdb
import os
import sys
from datetime import datetime
from pathlib import Path
import pandas as pd
import sqlparse
import traceback



sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import pipeline_lib.config as cfg

DATA_PARQUET_BASE_PATH = cfg.DATA_PARQUET_DIR_PATH
BASE_DIR = "/home/steco/quality_pipeline/pipeline_lib/sql"
QUERY_NAME = "smr-workflow"
BASE = "multi"
PROJECT_ID = "a01Hs00001ocUiNIAU"
REPORTING_WEEK = "04/18/2025"
TARGET = 0.9

class SafeDict(dict):
    def __missing__(self, key):
        return f"{{{key}}}"
    
def olap_rep_generate(query_name, base, project_id, reporting_week, target):
    base_dir = Path(BASE_DIR)
    print(f"BASE DIR: {base_dir}")

    if isinstance(base, str) and len(base)>0:
        base_code = base[0].upper()
    else:
        print(f"Olap generate: Invalid base code")
        return None
    
    query_map = {
            'smr-workflow': f'smr_workflow_{base}.sql',
            'smr-rater-label': f'query_{base}_smr_rater_label.sql',
            'smr-job-label': f'query_{base}_smr_job_label.sql',
            'smr-error-contribution': f'query_{base}_smr_error_contribution.sql',
            'dmp-job-incorrect': f'query_{base}_dmp_job_incorrect.sql'
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
    print(f"\nParsed Report Week to ISO: {reporting_week_str}")
    parquet_pattern = f"{project_id}/*/{project_id}_{reporting_week_str}_*_{base_code}_*.parquet"
    input_path = os.path.join(DATA_PARQUET_BASE_PATH, parquet_pattern)

    print(f"Parquet Input Path: {input_path}")
    
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
        traceback.print_exc()
        return None

    # Pretty-print with line numbers for debugging
    print("=== Rendered SQL ===")
    formatted = sqlparse.format(rendered_sql, reindent=True, keyword_case="upper")
    for i, line in enumerate(formatted.splitlines(), start=1):
        print(f"{i:03d}: {line}")
    print("====================")

    # Try executing
    try:
        return duckdb.query(rendered_sql).to_df()
    except Exception as e:
        print(f"[ERROR] Query execution failed: {e}")
        traceback.print_exc()
        
        """
        # Dump the formatted SQL to file for post-mortem
        dump_path = Path("failed_query.sql")
        try:
            with dump_path.open("w") as f:
                f.write(formatted)
            print(f"Failed SQL written to {dump_path.resolve()}")
        except Exception as write_err:
            print(f"Could not write failed SQL to disk: {write_err}")
        """

        return None


if __name__ == "__main__":
    test = olap_rep_generate(QUERY_NAME, BASE, PROJECT_ID, REPORTING_WEEK, TARGET)
    if test is not None:
        print(test.head())
        test.to_csv('testquery_result.csv', index=False)
    else:
        print("Query generation/execution failed.")