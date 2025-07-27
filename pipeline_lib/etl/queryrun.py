import duckdb
import os
from datetime import datetime
from pathlib import Path
import pipeline_lib.config as cfg

DATA_PARQUET_BASE_PATH = cfg.DATA_PARQUET_DIR_PATH

def query_run(query_name, project_id, content_week, target, methodology):
    base_dir = Path(__file__).parent

    cte_path = base_dir / "cte.sql"
    with cte_path.open("r") as f:
        cte_sql = f.read()

    query_map = {
        'smr-workflow': 'query_smr_workflow.sql',
        'smr-rater-label': 'query_smr_rater_label.sql',
        'smr-job-label': 'query_smr_job_label.sql',
        'smr-error-contribution': 'query_smr_error_contribution.sql',
        'job-incorrect': 'query_job_incorrect.sql'
    }

    if query_name not in query_map:
        print(f"[ERROR] Query '{query_name}' doesn't exist")

    query_file = base_dir / query_map[query_name]
    with query_file.open("r") as f:
        query_sql = f.read()

    # Build path
    week_str = content_week.strftime("%Y%m%d") if hasattr(content_week, "strftime") else str(content_week).replace("-", "")
    input_path = os.path.join(DATA_PARQUET_BASE_PATH, f"Parq_{project_id}/*/*C{week_str}.parquet")

    # Join CTE and Query
    full_sql = cte_sql + "\n" + query_sql

    rendered_sql = full_sql.format(
        input_path=f"'{input_path}'",
        project_id=f"'{project_id}'",
        content_week=f"'{content_week}'",
        target=target or '',
        methodology=f"'{methodology}'"
    )

    #print(f"Rendered SQL: {rendered_sql}")
    
    # Execute query
    return duckdb.query(rendered_sql).to_df()

