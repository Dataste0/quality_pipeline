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
HALO_MANUAL_SCORE_COL_NAME = "Vendor Manual QA Score"

INFO_COLUMN_RULES = {
    "rater_id": {
        "config_key": "rater_id_column",
        "fallbacks": [HALO_RATER_ID_COL_NAME],
        "required": True
    },
    "auditor_id": {
        "config_key": "auditor_id_column",
        "fallbacks": [HALO_AUDITOR_ID_COL_NAME],
        "required": True
    },
    "job_date": {
        "config_key": "submission_date_column",
        "fallbacks": [HALO_JOB_DATE_COL_NAME],
        "required": True
    },
    "job_id": {
        "config_key": "job_id_column",
        "fallbacks": [HALO_JOB_ID_COL_NAME],
        "required": True
    },
    "vendor_tag": {
        "config_key": "vendor_tag_column",
        "fallbacks": [HALO_VENDOR_TAG_COL_NAME],
        "required": True
    },
    "workflow": {
        "config_key": "workflow_column",
        "fallbacks": [HALO_WORKFLOW_COL_NAME, HALO_WORKFLOW_COL_NAME_FALLBACK],
        "required": False,   # created if missing
    },
    "manual_score": {
        "config_key": "manual_score_column",
        "fallbacks": [HALO_MANUAL_SCORE_COL_NAME],
        "required": False,   # can be completely missing
    },
}




def halo_transform(df, stats, mod_config):
    """
    MOD-CONFIG DICTIONARY FORMAT

    module_config = {
            "info_columns": { # Optional
                "rater_id_column": "SRT Annotator ID",
                "auditor_id_column": "Vendor Auditor ID",
                "job_id_column": "SRT Job ID",
                "submission_date_column": "Time (PT)",
                "workflow_column": "Queue Name",
                "vendor_tag_column": "Vendor Tag"
                "manual_score_column": "Vendor Manual QA Score",

            },

            "rubric": [ # Optional
                {
                    "rubric_column": "Mentions the Advertiser",
                    "rubric_name": "mentions_advertiser",
                    "rubric_penalty": 0.25,
                }
            ]

    }
    """

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
                stats["manual_score"] = "no manual score column"
                continue

        if found:
            info_columns_map[found] = std_name

    df.rename(columns=info_columns_map, inplace=True)



    # RUBRIC COLUMNS
    rubric_list = mod_config.get("rubric", [])
    
    # Add default rubric column
    rubric_list.append({
        "rubric_column": "default_rubric",
        "rubric_name": "default_rubric",
        "rubric_penalty": -1.0,
    })
    df["default_rubric"] = 1  # Default rubric column with value 1
    
    rubric_map = {
        f"{item.get('rubric_column')}": f"r_{item.get('rubric_name')}"
        for item in rubric_list
    }

    valid_rubric_map = {src: dst for src, dst in rubric_map.items() if src in df.columns}
    df.rename(columns=valid_rubric_map, inplace=True)



    # Remove other columns
    keep_cols_list = list(info_columns_map.values()) + list(valid_rubric_map.values())
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
    df["job_correct"] = df["vendor_tag"].eq("Approved").where(df["job_audited"], pd.NA)

    # if job has manual score, include it
    if "manual_score" in df.columns:
        df["job_manual_score"] = pd.to_numeric(df["manual_score"], errors="coerce") / 100
    else:
        df["job_manual_score"] = pd.NA


    # Keep only relevant columns
    col_list_keep = ['workflow', 'job_date', 'rater_id', 'auditor_id', 'job_id', 'job_correct', 'job_manual_score'] + list(valid_rubric_map.values())
    df = df[col_list_keep]

    # OUTPUT: ['workflow', 'job_date', 'rater_id', 'auditor_id', 'job_id', 'job_correct', 'job_manual_score'] [r_rubric1, r_rubric2, ... r_default_rubric]

    # Unpivot rubric
    df_long = (
        df.melt(
            id_vars=["workflow", "job_date", "rater_id", "auditor_id", "job_id", "job_correct", "job_manual_score"],
            value_vars=list(valid_rubric_map.values()),
            var_name="rubric",
            value_name="factor"
        )
        .assign(
            rubric=lambda d: d["rubric"].str.removeprefix("r_"), # Remove r_ prefix from column
            factor=lambda d: pd.to_numeric(d["factor"], errors="coerce").fillna(0).astype(int)
        )
    )

    # Assign penalties
    def norm_penalty(x: object) -> float:
        s = str(x).strip().rstrip('%').replace(',', '.')
        p = float(s)
        return p / 100 if p >= 1.0 else p
    penalty_map = {
        item['rubric_name']: norm_penalty(item['rubric_penalty'])
        for item in rubric_list
        if item.get('rubric_name') and item.get('rubric_penalty') not in (None, '')
    }
    df_long["rubric_penalty"] = df_long["rubric"].map(penalty_map).fillna(0.0)
    df_long["rubric_score"] = (df_long["factor"] * -1 * df_long["rubric_penalty"]).round(3)

    df = df_long.copy()

    # OUTPUT
    # ['workflow', 'job_date', 'rater_id', 'auditor_id', 'job_id', 'job_correct', 'job_manual_score'] [rubric, factor, rubric_penalty, rubric_score]

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
    stats["rows_after_transformation"] = len(df) if df is not None else 0


    return df, stats
