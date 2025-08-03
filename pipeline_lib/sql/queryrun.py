import duckdb
import os
from datetime import datetime
from pathlib import Path
import pipeline_lib.config as cfg

DATA_PARQUET_BASE_PATH = cfg.DATA_PARQUET_DIR_PATH

def olap_rep_generate(query_name, base, project_id, report_week, target):
    base_dir = Path(__file__).parent

    if isinstance(base, str) and len(base)>0:
        base_code = base[0].upper()
    else:
        print(f"Olap generate: Invalid base code")
        return None
    
    query_map = {
            'smr-workflow': f'query_{base}_smr_workflow.sql',
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
    #week_str = report_week.strftime("%Y%m%d") if hasattr(report_week, "strftime") else str(report_week).replace("-", "")
    report_week_str = report_week.strftime("%Y-%m-%d") if hasattr(report_week, "strftime") else str(report_week)
    input_path = os.path.join(DATA_PARQUET_BASE_PATH, f"{project_id}/*/{project_id}_{report_week}_*_{base_code}_*.parquet")

    # Rendered SQL
    rendered_sql = query_sql.format(
        input_path=f"'{input_path}'",
        project_id=f"'{project_id}'",
        reporting_week=f"'{report_week_str}'",
        target=target or '',
        base=f"'{base}'"
    )

    return duckdb.query(rendered_sql).to_df()



