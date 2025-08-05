import duckdb
import os
from pathlib import Path
import pipeline_lib.config as cfg
import pandas as pd

DATA_PARQUET_BASE_PATH = cfg.DATA_PARQUET_DIR_PATH

def query_map_info_run(item_to_collect="market"):

    dfs = []
    for base_code in ['A', 'M', 'R']:
        # costruisci il pattern del file parquet
        pattern = f"*/*/*_{base_code}_*.parquet"
        input_path = os.path.join(DATA_PARQUET_BASE_PATH, pattern)
            
        # definisci la query includendo il base_code per tracciare da quale schema viene
        sql_market = f"""
            SELECT
                project_id,
                workflow
            FROM '{input_path}'
            GROUP BY project_id, workflow
            """

        sql_label = f"""
            SELECT
                project_id,
                parent_label
            FROM '{input_path}'
            GROUP BY project_id, parent_label
            """
            
        sql_rubric = f"""
            SELECT
                project_id,
                rubric
            FROM '{input_path}'
            GROUP BY project_id, rubric
            """

        # esegui e raccogli
        if item_to_collect == "market":
            query = sql_market
            df = duckdb.query(query).to_df()
            required_columns = ['project_id', 'workflow']
            df = df[required_columns]
            dfs.append(df)
        
        elif item_to_collect == "label":
            #print(f"Collecting labels for base code: {base_code}")
            if base_code == 'R':
                #print("Collecting rubric labels")
                query = sql_rubric
                df = duckdb.query(query).to_df()
                required_columns = ['project_id', 'rubric']
                df = df[required_columns]
                df.rename(columns={'rubric': 'label'}, inplace=True)
                dfs.append(df)
            
            else:
                query = sql_label
                df = duckdb.query(query).to_df()
                required_columns = ['project_id', 'parent_label']
                df = df[required_columns]
                df.rename(columns={'parent_label': 'label'}, inplace=True)
                dfs.append(df)
        
        else:
            raise ValueError(f"Unknown collect type: {item_to_collect}")
    
    # unisci tutti i risultati
    if dfs:
        df = pd.concat(dfs, ignore_index=True)
    else:
        if item_to_collect == "market":
            df = pd.DataFrame(columns=['project_id', 'workflow'])
        elif item_to_collect == "label":
            df = pd.DataFrame(columns=['project_id', 'label'])
        else:
            raise ValueError(f"Unknown collect type: {item_to_collect}")

    return df
