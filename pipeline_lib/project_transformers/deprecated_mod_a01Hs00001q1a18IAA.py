###################
# Project 'Churchill (Media)'
###################

import pandas as pd
import numpy as np
from pipeline_lib.project_transformers import transformer_utils
import logging

# --- Setup logger
logger = logging.getLogger('pipeline.transform_modules')



def adhoc_transform(df, binary_labels=None, excluded_labels=None):
    # Select relevant columns only
    df = df[["Annotator ID","Annotation Date And Time","Annotation Job ID","Auditor ID","Is Job Successful?"]].copy()

    # Rename columns
    df.rename(columns={
        'Annotator ID': 'rater_id',
        'Annotation Date And Time': 'submission_date',
        'Annotation Job ID': 'job_id',
        'Auditor ID': 'auditor_id',
        'Is Job Successful?': 'job_correct'
    }, inplace=True)

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
    df['submission_date'] = pd.to_datetime(df['submission_date'], format='mixed', dayfirst=False, errors='coerce').dt.strftime('%Y-%m-%d')

    rater_df = df.copy()
    rater_df['actor_id'] = rater_df['rater_id']
    rater_df['actor_type'] = 'rater'
    rater_df['question_number'] = '1'
    rater_df['parent_label'] = 'default_label'
    rater_df['response_data'] = True
    rater_df['is_audit'] = '0'
    rater_df['source_of_truth'] = '0'
    
    auditor_df = df.copy()
    auditor_df['actor_id'] = auditor_df['auditor_id']
    auditor_df['actor_type'] = 'auditor'
    auditor_df['question_number'] = '1'
    auditor_df['parent_label'] = 'default_label'
    auditor_df['response_data'] = auditor_df['job_correct']
    auditor_df['is_audit'] = '1'
    auditor_df['source_of_truth'] = '0'

    columns_to_keep = ['submission_date', 'job_id', 'actor_id', 'question_number', 'parent_label', 'response_data', 'is_audit', 'source_of_truth']

    df_combined = pd.concat([rater_df[columns_to_keep], auditor_df[columns_to_keep]], ignore_index=True)

    return df_combined



def transform(df, metadata):
    stats = {}
    stats["etl_module"] = "ADHOC-a01Hs00001q1a18IAA"
    stats["rows_before_transformation"] = len(df)

    binary_labels = transformer_utils.get_binary_labels(metadata)
    excluded_labels = transformer_utils.get_excluded_labels(metadata)
    use_extracted = transformer_utils.get_use_extracted(metadata)
    quality_methods = transformer_utils.get_quality_methods(metadata)

    df = adhoc_transform(df, binary_labels, excluded_labels)
    df = transformer_utils.enrich_dataframe_with_metadata(df, metadata)

    stats["rows_after_transformation"] = len(df)
    return df, stats