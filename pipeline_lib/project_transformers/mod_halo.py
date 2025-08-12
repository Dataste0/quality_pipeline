###################
# Halo Admin Project Transformer
###################

import pandas as pd
import numpy as np
from pipeline_lib.project_transformers import transformer_utils as tu

# --- Logger
import logging
logger = logging.getLogger(__name__)



# Halo Default Columns
HALO_RATER_ID_COL_NAME = "SRT Annotator ID"
HALO_AUDITOR_ID_COL_NAME = "Vendor Auditor ID"
HALO_JOB_ID_COL_NAME = "SRT Job ID"
HALO_JOB_DATE_COL_NAME = "Time (PT)"
HALO_VENDOR_TAG_COL_NAME = "Vendor Tag"

HALO_WORKFLOW_COL_NAME = "Rubric Name"
HALO_WORKFLOW_COL_NAME_FALLBACK = "Queue Name"


def halo_transform(df, stats, mod_config):
    """
    MOD-CONFIG DICTIONARY FORMAT

    module_config = {
              "info_columns": {
                  "rater_id_column": "SRT Annotator ID",
                  "auditor_id_column": "Vendor Auditor ID",
                  "job_id_column": "SRT Job ID",
                  "submission_date_column": "Time (PT)",
                  "workflow_column": "Rubric Name",
                  "vendor_tag_column": "Vendor Tag"
              }

          }
    """

    info = mod_config.get("info_columns", {})

    # Map Info Columns
    rater_id_col            = info.get("rater_id_column", None)
    auditor_id_col          = info.get("auditor_id_column", None)
    job_date_col            = info.get("submission_date_column", None)
    job_id_col              = info.get("job_id_column", None)
    workflow_col            = info.get("workflow_column", None)
    vendor_tag_col          = info.get("vendor_tag_column", None)

    df["wf_col_placeholder"] = "default_workflow"
    
    info_columns_map = {
        rater_id_col            : "rater_id",
        auditor_id_col          : "auditor_id",
        job_date_col            : "job_date",
        job_id_col              : "job_id",
        workflow_col            : "workflow",
        vendor_tag_col          : "vendor_tag"
    }
    if not rater_id_col:
        if HALO_RATER_ID_COL_NAME in df.columns:
            info_columns_map[HALO_RATER_ID_COL_NAME] = "rater_id"
        else:
            print(f"ERROR: Unable to map Rater ID column")
            stats["transform_error"] = "Unable to map Rater ID column"
            return None
    
    if not auditor_id_col:
        if HALO_AUDITOR_ID_COL_NAME in df.columns:
            info_columns_map[HALO_AUDITOR_ID_COL_NAME] = "auditor_id"
        else:
            print(f"ERROR: Unable to map Auditor ID column")
            stats["transform_error"] = "Unable to map Auditor ID column"
            return None
    
    if not job_date_col:
        if HALO_JOB_DATE_COL_NAME in df.columns:
            info_columns_map[HALO_JOB_DATE_COL_NAME] = "job_date"
        else:
            print(f"ERROR: Unable to map Job Date column")
            stats["transform_error"] = "Unable to map Job Date column"
            return None
    
    if not job_id_col:
        if HALO_JOB_ID_COL_NAME in df.columns:
            info_columns_map[HALO_JOB_ID_COL_NAME] = "job_id"
        else:
            print(f"ERROR: Unable to map Job ID column")
            stats["transform_error"] = "Unable to map Job ID column"
            return None

    if not vendor_tag_col:
        if HALO_VENDOR_TAG_COL_NAME in df.columns:
            info_columns_map[HALO_VENDOR_TAG_COL_NAME] = "vendor_tag"
        else:
            print(f"ERROR: Unable to map Vendor Tag column")
            stats["transform_error"] = "Unable to map Vendor Tag column"
            return None

    if not workflow_col:
        if HALO_WORKFLOW_COL_NAME in df.columns:
            info_columns_map[HALO_WORKFLOW_COL_NAME] = "workflow"
        else:
            if HALO_WORKFLOW_COL_NAME_FALLBACK in df.columns:
                info_columns_map[HALO_WORKFLOW_COL_NAME_FALLBACK] = "workflow"
            else:
                info_columns_map["wf_col_placeholder"] = "workflow"


    info_valid_map = {src: dst for src, dst in info_columns_map.items() if src in df.columns}
    df.rename(columns=info_valid_map, inplace=True)
    
    # Remove other columns
    info_col_list = list(info_valid_map.values())
    df = df[info_col_list].copy()
    

    # Fix date format and remove rows with incorrect dates
    df["job_date"] = df["job_date"].apply(tu.convert_tricky_date)
    # Count excluded rows
    stats["skipped_invalid_datetime"] = int(df["job_date"].isnull().sum())
    # Remove exluded rows
    df = df[df["job_date"].notnull()].copy()


    # Strip quotes from ID Columns
    for col in ["rater_id", "auditor_id", "job_id"]:
        df[col] = df[col].astype("string").str.strip("'")
    

    # ID Format check
    df['rater_id'] = df['rater_id'].apply(tu.id_format_check)
    df['auditor_id'] = df['auditor_id'].apply(tu.id_format_check)
    df['job_id'] = df['job_id'].apply(tu.id_format_check)
    # Count invalid IDs
    mask_invalid_id = df[['rater_id', 'auditor_id', 'job_id']].isnull().any(axis=1)
    stats["skipped_invalid_id"] = int(mask_invalid_id.sum())
    # Remove from df
    df = df[~mask_invalid_id].copy()


    # Calculating Outcome
    df["vendor_tag"] = df["vendor_tag"].astype(str).str.strip().replace('nan', '')


    # if vendor tag is empty, job_audited is False else True
    df["job_audited"] = np.where(
        df["vendor_tag"].fillna("") != "",
        True,
        False,
    )

    # Count jobs not audited
    jobs_not_audited = (df["job_audited"] == False).sum()
    stats["jobs_not_audited"] = int(jobs_not_audited)

    # Keep audited only
    df = df[df["job_audited"] == True].copy()

    # if job is audited, check if vendor tag is "Approved" then True else False
    df["job_correct"] = np.where(
        df["job_audited"] == False,
        pd.NA,
        np.where(df["vendor_tag"] == "Approved", True, False)
    )

    
    col_list_keep = ['workflow', 'job_date', 'rater_id', 'auditor_id', 'job_id', 'job_correct']
    
    df = df[col_list_keep]

    # OUTPUT: ['workflow', 'job_date', 'rater_id', 'auditor_id', 'job_id', 'job_correct']

    return df



def transform(df, project_metadata):
    stats = {}
    stats["etl_module"] = "HALO"
    stats["rows_before_transformation"] = len(df)

    # Module config
    project_config = project_metadata.get("project_config", {})
    module_config = project_config.get("module_config", {})

    stats["rows_before_transformation"] = len(df)
    df = halo_transform(df, stats, module_config)
    stats["rows_after_transformation"] = len(df)


    return df, stats
