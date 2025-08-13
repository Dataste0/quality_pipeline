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

REPORTING_WEEK = "2025-08-01"
PROJECT_ID = "a01TR00000NBVZbYAP"
TARGET = 0.9
BASE = "R"


class SafeDict(dict):
    def __missing__(self, key):
        return f"{{{key}}}"


# Build path
project_id = PROJECT_ID
reporting_week = REPORTING_WEEK
base = BASE
target = TARGET
base_code = base[0].upper()

reporting_week_str = pd.to_datetime(reporting_week, errors="coerce").strftime("%Y-%m-%d")
parquet_pattern = f"{project_id}/*/{project_id}_{reporting_week_str}_*_{base_code}_*.parquet"
input_path = os.path.join(DATA_PARQUET_BASE_PATH, parquet_pattern)


params = {
    "input_path": f"'{input_path}'",
    "project_id": f"'{project_id}'",
    "reporting_week": f"'{reporting_week_str}'",
    "target": target or '',
    "base": f"'{base}'"
}

query_sql = """
WITH alldata AS (
    SELECT 
        *, 
        reporting_week as week_ending
    FROM (
        SELECT *, 
               ROW_NUMBER() OVER (
                   PARTITION BY project_id, job_id, rater_id, rubric
               ) AS row_num
        FROM {input_path}
        WHERE rubric IS NOT NULL 
          AND rubric <> '' 
          AND rubric <> 'pipeline_error'
          AND project_id = {project_id}
          AND reporting_week = {reporting_week}
    ) t
    WHERE row_num = 1
)
,

-- REPORT
rater_weekly_jobs AS (
    SELECT 
        week_ending,
        project_id,
        workflow,
        rater_id,
        auditor_id,
        job_id,
        job_correct,
        job_score
    FROM alldata
    GROUP BY week_ending, project_id, workflow, rater_id, auditor_id, job_id, job_correct, job_score
),

rater_weekly_info AS (
    SELECT 
        week_ending,
        project_id,
        workflow,
        rater_id,
        COUNT(DISTINCT job_id) AS rated_jobs,
        'default_rubric' as rubric,
        COUNT(DISTINCT job_id) AS total_label_count,
        SUM(CASE WHEN job_correct THEN 1 ELSE 0 END) AS total_correct_label_count,
        0::INT AS tp_count,
        0::INT AS tn_count,
        0::INT AS fp_count,
        0::INT AS fn_count
    FROM rater_weekly_jobs
    GROUP BY week_ending, project_id, workflow, rater_id, 'default_rubric'
),

rater_weekly_score AS (
    SELECT
        *,
        CASE WHEN total_label_count = 0 THEN NULL ELSE total_correct_label_count/total_label_count::FLOAT END AS r_label_score,
        0::FLOAT AS r_label_f1score,
        0::FLOAT AS r_label_precision,
        0::FLOAT AS r_label_recall,
        CASE WHEN total_label_count = 0 THEN NULL ELSE total_correct_label_count/total_label_count::FLOAT END AS r_overall_score,
        0::FLOAT AS r_overall_f1score,
        0::FLOAT AS r_overall_precision,
        0::FLOAT AS r_overall_recall
    FROM rater_weekly_info

)


SELECT * FROM rater_weekly_score

"""
   
   

try:
    rendered_sql = query_sql.format_map(SafeDict(params))
except Exception as e:
    print(f"[ERROR] Failed to format SQL template: {e}")
    print(f"Query: {query_map[query_name]} - Params: {params}")
    traceback.print_exc()
    exit(1)


# Pretty-print with line numbers for debugging
print("=== Rendered SQL ===")
formatted = sqlparse.format(rendered_sql, reindent=True, keyword_case="upper")
for i, line in enumerate(formatted.splitlines(), start=1):
    print(f"{i:03d}: {line}")
print("====================")



    # Try executing
try:
    df = duckdb.query(rendered_sql).to_df()
    df.to_csv('query_test_output.csv', index=False)
    
except Exception as e:
    print(f"[ERROR] Query execution failed: {e}")
    print(f"Query: {rendered_sql}")
    traceback.print_exc()
    exit(1)
