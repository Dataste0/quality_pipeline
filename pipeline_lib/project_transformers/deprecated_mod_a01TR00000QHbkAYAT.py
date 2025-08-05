###################
# Project 'Lantana'
###################

import pandas as pd
from pipeline_lib.project_transformers import transformer_utils

# --- Logger
import logging
logger = logging.getLogger(__name__)



def adhoc_transform(df, stats, binary_labels=None, excluded_labels=None):
    # Select relevant columns only
    df = df[["SRT Annotator ID", "Time (PT)", "SRT Job ID", "Vendor Auditor ID", "Vendor Comment"]].copy()

    # Rename columns
    df.rename(columns={
        'SRT Annotator ID': 'rater_id',
        'Time (PT)': 'submission_date',
        'SRT Job ID': 'job_id',
        'Vendor Auditor ID': 'auditor_id',
        'Vendor Comment': 'auditor_comment'
    }, inplace=True)

    # Fix date format and remove rows with incorrect dates
    df['submission_date'] = df['submission_date'].apply(transformer_utils.convert_tricky_date)
    # Count excluded rows
    stats["skipped_invalid_datetime"] = int(df['submission_date'].isnull().sum())
    # Remove exluded rows
    df = df[df['submission_date'].notnull()].copy()

    # Strip single quotes
    columns_to_strip = ['rater_id', 'submission_date', 'job_id', 'auditor_id']
    for col in columns_to_strip:
        df[col] = df[col].astype(str).str.strip("'")
    
    # If auditor didn't provide comment, the job is correct
    df['job_correct'] = df['auditor_comment'].astype(str).str.strip().replace('nan', '').eq('')


    # ID Format check
    df['rater_id'] = df['rater_id'].apply(transformer_utils.id_format_check)
    df['auditor_id'] = df['auditor_id'].apply(transformer_utils.id_format_check)
    df['job_id'] = df['job_id'].apply(transformer_utils.id_format_check)
    # Count invalid IDs
    mask_invalid_id = df[['rater_id', 'auditor_id', 'job_id']].isnull().any(axis=1)
    stats["skipped_invalid_id"] = int(mask_invalid_id.sum())
    # Remove from df
    df = df[~mask_invalid_id].copy()


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
    stats["etl_module"] = "ADHOC-a01TR00000QHbkAYAT"
    stats["rows_before_transformation"] = len(df)

    binary_labels = transformer_utils.get_binary_labels(metadata)
    excluded_labels = transformer_utils.get_excluded_labels(metadata)

    df = adhoc_transform(df, stats, binary_labels, excluded_labels)
    df = transformer_utils.enrich_dataframe_with_metadata(df, metadata)

    stats["rows_after_transformation"] = len(df)

    return df, stats