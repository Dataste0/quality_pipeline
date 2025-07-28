###################
# Project 'Churchill Correctness - Media'
###################

import pandas as pd
from pipeline_lib.project_transformers import transformer_utils
import logging

# --- Setup logger
logger = logging.getLogger('pipeline.transform_modules')



def adhoc_transform(df, reporting_week, excluded_labels=None):
    df['submission_date'] = pd.to_datetime(reporting_week, errors="coerce").strftime('%Y-%m-%d')

    # Select relevant columns only
    df = df[["job_id","reviewer_id","question","answer","submission_date"]].copy()

    # Rename columns
    df.rename(columns={
        'reviewer_id': 'actor_id',
        'question': 'parent_label',
        'answer': 'response_data'
    }, inplace=True)

    df['is_audit'] = '0'
    df['source_of_truth'] = '0'

    # Filter excluded labels
    if excluded_labels:
        df_filtered = df[~df['parent_label'].isin(excluded_labels)]
    else:
        df_filtered = df

    return df_filtered.copy()



def transform(df, metadata):
    stats = {}
    stats["etl_module"] = "ADHOC-a01Hs00001q1a2uIAA"
    stats["rows_before_transformation"] = len(df)

    excluded_labels = transformer_utils.get_excluded_labels(metadata)

    if metadata.get("use_reporting_data") is True:
        if "reporting_week" not in metadata:
            stats["reporting_week_missing"] = True
        else:
            reporting_week = metadata["reporting_week"]

    df = adhoc_transform(df, reporting_week, excluded_labels)
    df = transformer_utils.enrich_dataframe_with_metadata(df, metadata)

    stats["rows_after_transformation"] = len(df)
    return df, stats