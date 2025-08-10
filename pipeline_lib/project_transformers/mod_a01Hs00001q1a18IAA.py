###################
# Project 'Churchill (Media)'
###################

import pandas as pd
import numpy as np
from pipeline_lib.project_transformers.transformer_utils import convert_tricky_date
from pipeline_lib.project_transformers.base_audit import base_transform as baudit

# --- Logger
import logging
logger = logging.getLogger(__name__)



def adhoc_transform(df, stats):
    # Select relevant columns only
    df = df[["Annotator ID","Annotation Date And Time","Annotation Job ID","Auditor ID","Is Job Successful?"]].copy()

    # Rename columns
    df.rename(columns={
        'Annotator ID': 'rater_id',
        'Annotation Date And Time': 'job_date',
        'Annotation Job ID': 'job_id',
        'Auditor ID': 'auditor_id',
        'Is Job Successful?': 'job_correct'
    }, inplace=True)

    # Add workflow
    df['workflow'] = 'default_workflow'

    # Select audited jobs only
    df = df[df["job_correct"].notna() & (df["job_correct"].astype(str).str.strip() != "")].copy()

    # Filter out Annotator ID empty
    df = df[df["rater_id"].notna() & (df["rater_id"].astype(str).str.strip() != "")].copy()


    # Normalize true/false values
    df['job_correct'] = (
        df['job_correct']
        .astype(str)
        .str.lower()
        .str.strip()
        .map({'true': True, 'false': False, 'yes': True, 'no': False})
    )

    

    # Fix date format
    df['job_date'] = df['job_date'].apply(convert_tricky_date)

    df['rater_is_job_successful'] = 'true'
    df['auditor_is_job_successful'] = df['job_correct'].astype(str).str.lower()

    columns_to_keep = ['workflow', 'job_date', 'job_id', 'rater_id', 'auditor_id', 'rater_is_job_successful', 'auditor_is_job_successful']

    df = df[columns_to_keep].copy()


    return df



def transform(df, metadata):
    stats = {}
    stats["etl_module"] = "ADHOC-a01Hs00001q1a18IAA"

    stats["rows_before_transformation"] = len(df)
    df = adhoc_transform(df, stats)
    stats["rows_after_transformation"] = len(df)

    module_info = {}
    module_info["base_config"] = {
        "labels": [
            {
                "label_name": "is_job_successful",
                "is_label_binary": False,
                "weight": 1,
                "auditor_column_type": "answer"
            }
        ]
    }

    base_config = module_info.get("base_config", {})
    
    base_df, base_info = baudit(df, base_config)
    
    stats["base_info"] = base_info

    return base_df, stats