###################
# Spotcheck Excel Transformer
###################

import pandas as pd
import numpy as np
from pipeline_lib.project_transformers import transformer_utils as tu

# --- Logger
import logging
logger = logging.getLogger(__name__)



# Default Columns
ACTOR_ID_COL_NAME = "actor_id"
ACTOR_RESPONSE_COL_NAME = "actor_answer"
REVIEWER_ID_COL_NAME = "reviewer_id"
REVIEWER_RESPONSE_COL_NAME = "reviewer_answer"
JOB_ID_COL_NAME = "job_id"
JOB_DATE_COL_NAME = "job_date"
WORKFLOW_COL_NAME = "queue"
LABEL_COL_NAME = "label"


INFO_COLUMN_RULES = {
    "actor_id": {
        "config_key": "actor_id_column",
        "fallbacks": [ACTOR_ID_COL_NAME],
        "required": True
    },
    "reviewer_id": {
        "config_key": "reviewer_id_column",
        "fallbacks": [REVIEWER_ID_COL_NAME],
        "required": True
    },
    "actor_response": {
        "config_key": "actor_response_column",
        "fallbacks": [ACTOR_RESPONSE_COL_NAME],
        "required": True
    },
    "reviewer_response": {
        "config_key": "reviewer_response_column",
        "fallbacks": [REVIEWER_RESPONSE_COL_NAME],
        "required": True
    },
    "job_date": {
        "config_key": "job_date_column",
        "fallbacks": [JOB_DATE_COL_NAME],
        "required": True
    },
    "job_id": {
        "config_key": "job_id_column",
        "fallbacks": [JOB_ID_COL_NAME],
        "required": True
    },
    "workflow": {
        "config_key": "workflow_column",
        "fallbacks": [WORKFLOW_COL_NAME],
        "required": False   # created if missing
    },
    "parent_label": {
        "config_key": "label_column",
        "fallbacks": [LABEL_COL_NAME],
        "required": True
    },
}




def spotcheck_transform(df, stats, mod_config):
    """
    MOD-CONFIG DICTIONARY FORMAT

    module_config = {
            "info_columns": { # Optional
                "actor_id_column": "auditor_id",
                "actor_response_column": "auditor_answer",
                "reviewer_id_column": "reviewer_id",
                "reviewer_response_column": "ground_truth",
                "job_id_column": "job_id",
                "job_date_column": "review_date",
                "workflow_column": "queue",
                "label_column": "label"

            }

    }
    """

    stats["rows_initial"] = len(df)

    binary_labels = mod_config.get("binary_labels", [])
    label_weights = mod_config.get("label_weights", {})

    stats["binary_labels"] = binary_labels
    stats["label_weights"] = label_weights


    # INFO COLUMNS
    info = mod_config.get("info_columns", {})
    info_columns_map = {}

    for std_name, rule in INFO_COLUMN_RULES.items():
        col_name = info.get(rule["config_key"])
        found = None

        if col_name and col_name in df.columns:
            found = col_name
        else:
            for fb in rule.get("fallbacks", []):
                if fb in df.columns:
                    found = fb
                    break

        if not found:
            if rule["required"]:
                stats["transform_error"] = f"Unable to map {std_name} column"
                print(stats["transform_error"])
                return None
            elif std_name == "workflow":
                df["workflow"] = "default_workflow"
            else:
                continue

        if found:
            info_columns_map[found] = std_name

    df.rename(columns=info_columns_map, inplace=True)


    # Let's use standard naming rater-auditor like other modules
    df.rename(columns={
        "actor_id": "rater_id",
        "actor_response": "rater_response",
        "reviewer_id": "auditor_id",
        "reviewer_response": "auditor_response",
    }, inplace=True)


    # Remove other columns
    keep_cols_list = list(info_columns_map.values())
    df = df[keep_cols_list].copy()


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


    #AUDIT [workflow, job_date, rater_id, auditor_id, job_id] [parent_label] [rater_response, auditor_response]

    # Add binary flags
    df = tu.add_binary_flags(df, binary_labels)
    #[workflow, job_date, rater_id, auditor_id, job_id] [parent_label] [rater_response, auditor_response] [is_label_binary, confusion_type]

    df = tu.add_responses_match(df, col_name="is_correct", case_sensitive=False, strip=True)
    #[workflow, job_date, rater_id, auditor_id, job_id] [parent_label] [rater_response, auditor_response] [is_label_binary, confusion_type] [is_correct]

    #AUDIT [workflow, job_date, rater_id, auditor_id, job_id] [parent_label] [rater_response, auditor_response] [is_label_binary, confusion_type] [is_correct]

    df["weight"] = df["parent_label"].map(label_weights).fillna(1).astype(float)

    #AUDIT [workflow, job_date, rater_id, auditor_id, job_id] [parent_label] [rater_response, auditor_response] [is_label_binary, confusion_type] [is_correct] [weight]

    # Compile stats
    stats["rows_final"] = len(df)

    return df



def transform(df, project_metadata):
    stats = {}
    stats["etl_module"] = "SPOTCHECK"
    stats["rows_before_transformation"] = len(df)

    # Module config
    project_config = project_metadata.get("project_config", {})
    module_config = project_config.get("module_config", {})

    stats["rows_before_transformation"] = len(df)
    df = spotcheck_transform(df, stats, module_config)
    stats["rows_after_transformation"] = len(df) if df is not None else 0


    return df, stats
