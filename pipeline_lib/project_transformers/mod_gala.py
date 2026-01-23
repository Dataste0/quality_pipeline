###################
# Gala Project Transformer
###################

import re
import pandas as pd
import numpy as np
import json
import ast
from pipeline_lib.project_transformers import transformer_utils as tu

# --- Logger
import logging
logger = logging.getLogger(__name__)



# Halo Default Columns
GALA_RATER_ID_COL_NAME = "annotator_id"
GALA_AUDITOR_ID_COL_NAME = "auditor_id"
GALA_JOB_ID_COL_NAME = "task_id"
GALA_JOB_DATE_COL_NAME = "original_submission_time"
GALA_AUDIT_STATUS_COL_NAME = "audit_status"
GALA_WORKFLOW_COL_NAME = "task_name"
GALA_MANUAL_SCORE_COL_NAME = "QA_score"
GALA_RUBRIC_COL_NAME = "rubric_answer"

INFO_COLUMN_RULES = {
    "rater_id": {
        "config_key": "rater_id_column",
        "fallbacks": [GALA_RATER_ID_COL_NAME],
        "required": True
    },
    "auditor_id": {
        "config_key": "auditor_id_column",
        "fallbacks": [GALA_AUDITOR_ID_COL_NAME],
        "required": True
    },
    "job_date": {
        "config_key": "submission_date_column",
        "fallbacks": [GALA_JOB_DATE_COL_NAME],
        "required": True
    },
    "job_id": {
        "config_key": "job_id_column",
        "fallbacks": [GALA_JOB_ID_COL_NAME],
        "required": True
    },
    "audit_status": {
        "config_key": "audit_status_column",
        "fallbacks": [GALA_AUDIT_STATUS_COL_NAME],
        "required": True
    },
    "workflow": {
        "config_key": "workflow_column",
        "fallbacks": [GALA_WORKFLOW_COL_NAME],
        "required": False,   # created if missing
    },
    "manual_score": {
        "config_key": "manual_score_column",
        "fallbacks": [GALA_MANUAL_SCORE_COL_NAME],
        "required": False,   # can be completely missing
    },
    "job_rubric": {
        "config_key": "rubric_column",
        "fallbacks": [GALA_RUBRIC_COL_NAME],
        "required": False,   # can be completely missing
    },
}




def gala_transform(df, stats, mod_config):
    """
    MOD-CONFIG DICTIONARY FORMAT

    module_config = {
            "info_columns": { # Optional
                "rater_id_column": "annotator_id",
                "auditor_id_column": "auditor_id",
                "job_id_column": "task_id",
                "submission_date_column": "original_submission_time",
                "workflow_column": "task_name",
                "audit_status_column": "audit_status",
                "manual_score_column": "QA_score",
                "rubric_column": "rubric_answer",
            },

            "rubric": [ # Optional
                {
                    "rubric_extended": "Mentions the Advertiser",
                    "rubric_name": "mentions_advertiser",
                    "rubric_penalty": 25,
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



    # RUBRIC PROCESSING

    # Convert list of dicts in 'job_rubric' to proper dict in column 'rubric_temp_col'
    # [{"question": "Mentions the Advertiser", "answer": "[\"Yes\",\"Yes\"]"}, ...]  --> {"Mentions the Advertiser": 2, ...}

    def safe_json_loads(x):
        if pd.isna(x):
            return []
        if isinstance(x, (list, dict)):
            return x
        s = str(x).strip()
        if s == "" or s.lower() in {"nan", "none", "null"}:
            return []
        try:
            return json.loads(s)
        except json.JSONDecodeError:
            return []

    #df["job_rubric"] is an empty string or a json string representing list of dicts: converting to list of dicts
    df["job_rubric"] = df["job_rubric"].apply(safe_json_loads)


    # Evaluate rubric in data
    def count_yes(serialized):
        if not serialized:
            return 0
        parsed = None
        # First try: JSON
        try:
            parsed = json.loads(serialized)
        except Exception:
            pass
        # Fallback: literal Python
        if parsed is None:
            try:
                parsed = ast.literal_eval(serialized)
            except Exception:
                return 0
        if not isinstance(parsed, list):
            return 0

        return sum(1 for x in parsed if x == "Yes")

    def build_rubric_temp(row):
        if not isinstance(row, list):
            return {}

        return {
            item["question"]: count_yes(item.get("answer"))
            for item in row
            if isinstance(item, dict) and "question" in item
        }
    
    df["rubric_temp_long"] = df["job_rubric"].apply(build_rubric_temp)

    # df["rubric_temp_long"] now has {"Mentions the Advertiser": 2, ...}

    
    # Euristic to determine rubric entries and penalties
    full_rubric = tu.generate_rubric(
        df=df,
        rubric_column="rubric_temp_long",
        score_column="manual_score",
        provided_rubric=mod_config.get("rubric", []),
        rubric_entries_cols=None,
        warn_on_conflicts=False
    )

    



    # Maps rubric full string to proper rubric name: e.g. "Mentions the Advertiser" --> "mentions_advertiser"    

    # Using provided rubric
    rubric_mapping = {item["rubric_extended"]: item["rubric_name"] for item in full_rubric if item.get("rubric_extended") and item.get("rubric_name")}

    # {rubric_extended: count}  -->  {rubric_name: count}
    df["rubric_temp_short"] = df["rubric_temp_long"].apply(
        lambda d: {
            rubric_mapping[k]: v
            for k, v in d.items()
            if k in rubric_mapping
        }
    )
    
    # df["rubric_temp_short"] now has {"mentions_advertiser": 2, ...}
    
    # Pull provided rubric in project metadata
    rubric_provided_list = [item["rubric_name"] for item in full_rubric]
    rubric_provided_set = set(rubric_provided_list)

    # Combine rubric row-level with complete set
    df["rubric_temp_short_complete"] = df["rubric_temp_short"].apply(
        lambda d: {
            k: d.get(k, 0) if isinstance(d, dict) else 0
            for k in rubric_provided_set
        }
    )

    # df["rubric_temp_short_complete"] now has all rubric entries with factor 0 if missing
    # example: {"mentions_advertiser": 2, "correct_spelling": 0, "uses_proper_format": 1, ...}

    # Expand dict into columns
    df_expanded = pd.json_normalize(df["rubric_temp_short_complete"])

    # Add default rubric item
    full_rubric.append({
        "rubric_extended": "default_rubric",
        "rubric_name": "default_rubric",
        "rubric_penalty": -100.0,
    })
    stats["rubric_used"] = full_rubric

    df_expanded["default_rubric"] = 1  # Default rubric column with value 1

    # Merge expanded rubric columns back to main df
    df = pd.concat([df, df_expanded], axis=1)
    
    rubric_col_name_map = {
        item["rubric_name"]: f"r_{item['rubric_name']}"
        for item in full_rubric
        if item.get("rubric_name")
    }
    valid_rubric_map = {src: dst for src, dst in rubric_col_name_map.items() if src in df.columns}
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
    df["audit_status"] = df["audit_status"].astype(str).str.strip().replace('nan', '')

    # if vendor tag is empty, job_audited is False else True
    df["job_audited"] = np.where(
        df["audit_status"].fillna("") != "",
        True,
        False,
    )

    # Count jobs not audited
    jobs_not_audited = (df["job_audited"] == False).sum()
    stats["jobs_not_audited"] = int(jobs_not_audited)

    # Keep audited only
    df = df[df["job_audited"] == True].copy()

    # if job is audited, check if vendor tag is "AUDIT_APPROVED" then True else False
    df["job_correct"] = df["audit_status"].eq("AUDIT_APPROVED").where(df["job_audited"], pd.NA)

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
        for item in full_rubric
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
    stats["etl_module"] = "GALA"
    stats["rows_before_transformation"] = len(df)

    # Module config
    project_config = project_metadata.get("project_config", {})
    module_config = project_config.get("module_config", {})

    stats["rows_before_transformation"] = len(df)
    df = gala_transform(df, stats, module_config)
    stats["rows_after_transformation"] = len(df) if df is not None else 0


    return df, stats
