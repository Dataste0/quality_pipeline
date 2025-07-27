import duckdb
import os
from datetime import datetime
from pathlib import Path
import pipeline_lib.config as cfg

DATA_PARQUET_BASE_PATH = cfg.DATA_PARQUET_DIR_PATH

def query_map_info_run(query_name):
    base_dir = Path(__file__).parent

    query_map = {
        'map-info-markets': 'query_map_info_markets.sql',
        'map-info-labels': 'query_map_info_labels.sql'
    }

    if query_name not in query_map:
        print(f"[ERROR] Query '{query_name}' doesn't exist")

    query_file = base_dir / query_map[query_name]
    with query_file.open("r") as f:
        query_sql = f.read()

    # Build path
    input_path = os.path.join(DATA_PARQUET_BASE_PATH, f"Parq_*/*/*C*.parquet")

    # Join CTE and Query
    full_sql = query_sql

    rendered_sql = full_sql.format(
        input_path=f"'{input_path}'",
    )

    #print(f"Rendered SQL: {rendered_sql}")
    
    # Execute query
    return duckdb.query(rendered_sql).to_df()

