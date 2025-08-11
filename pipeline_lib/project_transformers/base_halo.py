###############
# BASE - HALO
###############

# INPUT Taken
#                       [workflow, job_date, rater_id, auditor_id, job_id, job_correct]

# OUTPUT
# [base, content_week]  [workflow, job_date, rater_id, auditor_id, job_id, job_correct]


import pandas as pd
import numpy as np

from pipeline_lib.project_transformers import transformer_utils

MODEL_BASE = "halo"



def base_halo_etl(df, stats, base_config):

    try:

        # Needed columns
        base_cols_list = ['workflow', 'job_date', 'rater_id', 'auditor_id', 'job_id', 'job_correct']

        required_cols = base_cols_list
        # Verifica che esistano nel df
        missing_cols = [col for col in required_cols if col not in df.columns]
        if missing_cols:
            stats["transform_error"] = f"Missing required info columns in DataFrame: {missing_cols}"
            print(stats["transform_error"])
            return None

        df = df[required_cols].copy()

        # Strip quotes from ID Columns (preserve NA)
        for col in ["rater_id", "auditor_id", "job_id"]:
            df[col] = df[col].where(df[col].isna(), df[col].astype(str).str.strip("'"))


        # Replace empty workflow with "default_workflow"
        df['workflow'] = df['workflow'].fillna('').astype(str)
        df.loc[df['workflow'].str.strip().str.lower() == '', 'workflow'] = 'default_workflow'
        
        # Replace empty auditor_id with Na
        df['auditor_id'] = df['auditor_id'].mask(df['auditor_id'].str.strip() == '', pd.NA)

        
        # Replace empty job_correct with NA
        df['job_correct'] = df['job_correct'].mask(
            df['job_correct'].astype(str).str.strip() == '',
            pd.NA
        )

        
        # Reorder
        final_cols = ['workflow', 'job_date', 'rater_id', 'auditor_id', 'job_id', 'job_correct']
        df = df[final_cols]

        # OUTPUT ['workflow', 'job_date', 'rater_id', 'auditor_id', 'job_id', 'job_correct']
        return df

    except Exception as e:
        stats["transform_error"] = f"Unexpected error: {str(e)}"
        print(f"BASE HALO ERROR: Unexpected error: {str(e)}")
        return None




def base_transform(df, base_config):
    stats = {}
    stats["model_base"] = f"BASE-{MODEL_BASE.upper()}"

    stats["rows_before_transformation"] = len(df)

    if not base_config:
        stats["transform_error"] = "base_config_missing"
        return pd.DataFrame(), stats
    
    df = base_halo_etl(df, stats, base_config)
    df.insert(0, "base", MODEL_BASE)
    df.insert(1, "content_week", transformer_utils.compute_content_week(df["job_date"]))

    stats["rows_after_transformation"] = len(df)

    # Add project_id...
    #df = transformer_utils.enrich_dataframe_with_metadata(df, metadata)


    return df, stats

