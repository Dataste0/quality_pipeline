###################
# Generic Project Transformer
###################

import pandas as pd
import numpy as np
from pipeline_lib.project_transformers import transformer_utils
from pipeline_lib.project_transformers.base_rubric import base_transform as brubric
import logging

# --- Setup logger
logger = logging.getLogger('pipeline.transform_modules')


# Tipicamente Halo
"""
definire singolarmente label e colonne: rater (colonne answer)
auditor (colonne answer/agreement/disagreement)
colonna outcome (definire valore correct/incorrect) se blank è non audited
definire score o penalty
"""


def generic_transform(df, stats, mod_config):
    """
    MOD-CONFIG DICTIONARY FORMAT

    mod_config = {
        "info_columns": {
            "rater_id_column": "Annotator ID",
            "auditor_id_column": "Vendor Auditor ID",
            "job_id_column": "SRT Job ID",
            "submission_date_column": "Time (PT)",
            "workflow_column": "Rubric Name",
        },

        "job_outcome": {
            # This column must have job_outcome_positive value for Pass or job_outcome_negative for Fail
            "job_outcome_column": "Vendor Tag",
            "job_outcome_type": "pass" / "fail",
            "pass_value": "YES",
            "pass_if_not_empty": True
            #"fail_value": "NO",
            #"fail_if_not_empty": False
        }

        "audited": {
            # This column must be not-empty to determine if the job is audited
            "audited_column": "Vendor Tag"
            "audited_if_not_empty": True
            # "audited_value":
        }

        "provided_score": {
            "use_provided_score": True,
            "provided_score_column": "column_name"
        }

        "top_score": 100,
        "bottom_score": 80,
        "default_label_name": "main_rubric",
        "use_job_score_provided": true,
        "use_job_outcome_provided": false,
        "pass_score": 100,
        "incorrect_if_commented": true,
              
        "labels": [
            {
                "label_name": "able_to_eval",
                "rater_label_column": "r_able_to_eval", 
                "auditor_label_column": "a_able_to_eval",
                "auditor_column_type": "answer", #"answer/disagreement/agreement",
                "agreement": {
                    "agreement_value":
                },
                "disagreement": {},
                "is_label_binary": true,
                "label_binary_pos_value": "EB_yes",
                "weight": null
            },
            {
                "label_name": "withhold",
                "rater_label_column": "r_withhold", 
                "auditor_label_column": "a_withhold",
                "auditor_column_type": "answer",
                "is_label_binary": false,
                "weight": null
            }
        ]
    }
    """

    info = mod_config.get("info_columns", None)
    if not info:
        stats["transformation_error"] = "config_error_undeclared_columns"
    
    #quality_methodology = mod_config.get("quality_methodology", None)
    #stats["quality_methodology"] = quality_methodology


    use_job_score_provided  = mod_config.get("use_job_score_provided", False)
    use_job_outcome_provided= mod_config.get("use_job_outcome_provided", False)
    incorrect_if_commented  = mod_config.get("incorrect_if_commented", False)
    top_score               = int(mod_config.get("top_score", 100)) /100
    bottom_score            = int(mod_config.get("bottom_score", 0)) /100
    pass_score              = int(mod_config.get("pass_score", 100)) /100
    default_rubric_name     = mod_config.get("default_rubric_name", "default_rubric")

    
    # Map Info Columns

    rater_id_col            = info.get("rater_id_column", HALO_RATER_ID_COL_NAME)
    auditor_id_col          = info.get("auditor_id_column", HALO_AUDITOR_ID_COL_NAME)
    job_date_col            = info.get("submission_date_column", HALO_JOB_DATE_COL_NAME)
    job_id_col              = info.get("job_id_column", HALO_JOB_ID_COL_NAME)
    workflow_col            = info.get("workflow_column", HALO_WORKFLOW_COL_NAME)
    vendor_tag_col          = info.get("vendor_tag_column", HALO_VENDOR_TAG_COL)
    vendor_comment_col      = info.get("vendor_comment_column", HALO_VENDOR_COMMENT_COL)
    vendor_score_col        = info.get("vendor_score_column", HALO_VENDOR_SCORE_COL)
    
    info_columns_map = {
        rater_id_col            : "rater_id",
        auditor_id_col          : "auditor_id",
        job_date_col            : "job_date",
        job_id_col              : "job_id",
        workflow_col            : "workflow",
    }
    if use_job_outcome_provided:
        if vendor_tag_col in df.columns:
            info_columns_map[vendor_tag_col] = "vendor_tag"
        else:
            stats["transformation_error"] = "vendor_tag_column_missing"
    if use_job_score_provided:
        if vendor_score_col in df.columns:
            info_columns_map[vendor_score_col] = "vendor_score"
        else:
            stats["transformation_error"] = "vendor_score_column_missing"
    if incorrect_if_commented:
        if vendor_comment_col in df.columns:
            info_columns_map[vendor_comment_col] = "vendor_comment"
        else:
            stats["transformation_error"] = "vendor_comment_column_missing"
    
    info_valid_map = {src: dst for src, dst in info_columns_map.items() if src in df.columns}
    df.rename(columns=info_valid_map, inplace=True)


    # Map Rubric Columns
    rubric_list_items = mod_config.get("rubric", [])
    rubric_col_map = {}
    for v in rubric_list_items:
        if not isinstance(v, dict):
            logging.debug("Skipping non-dict rubric item: %r", v)
            continue
        label_col = v.get("label_column")
        label_name = v.get("label_name")
        rubric_col_map[label_col] = f"rb_{label_name}"
    
    rub_valid_map = {src: dst for src, dst in rubric_col_map.items() if src in df.columns} # safely rename only existing columns
    df.rename(columns=rub_valid_map, inplace=True)

    # Saving rubric list
    stats["rubric_list"] = [r.get("label_name") for r in rubric_list_items]


    
    # Remove other columns
    info_col_list = list(info_valid_map.values())
    rub_col_list = list(rub_valid_map.values())
    keep = info_col_list + rub_col_list
    keep = [c for c in dict.fromkeys(keep) if c in df.columns]
    df = df[keep].copy()

    


    # Fix date format and remove rows with incorrect dates
    df["job_date"] = df["job_date"].apply(transformer_utils.convert_tricky_date)
    # Count excluded rows
    stats["skipped_invalid_datetime"] = int(df["job_date"].isnull().sum())
    # Remove exluded rows
    df = df[df["job_date"].notnull()].copy()


    # Calculating Job score
    if use_job_score_provided:
        df["job_score"] = pd.to_numeric(df["vendor_score"], errors="coerce") / 100

    else:
        #Building map label_column -> penalty_score
        rubric_items = mod_config.get("rubric", []) or []
        penalty_map = {
            v["label_column"]: float(v.get("penalty_score", v.get("penalty", 0))) / 100.0
            for v in rubric_items
            if "label_column" in v
        }
        valid_cols = [col for col in rub_col_list if col in penalty_map]
        if valid_cols:
            penalty_series = pd.Series({col: penalty_map[col] for col in valid_cols})
            # moltiplica le colonne per i rispettivi penalty e somma per riga
            total_penalty_series = (df[valid_cols] * penalty_series).sum(axis=1)
        else:
            total_penalty_series = 0

        df["job_score"] = top_score - total_penalty_series
        df["job_score"] = df["job_score"].clip(lower=bottom_score)


    # Calculating Outcome
    if use_job_outcome_provided:
        df["vendor_tag"] = df["vendor_tag"].astype(str).str.strip().replace('nan', '')
        df["job_audited"] = df["vendor_tag"].fillna("") != ""

        if incorrect_if_commented:
            # job_correct è True solo se Approved, audited e senza commento
            # normalizzi anche vendor_comment per confronto
            df["vendor_comment"] = df["vendor_comment"].astype(str).str.strip().replace("nan", "")

            df["job_correct"] = np.where(
                (df["vendor_tag"] == "Approved") & df["job_audited"] & (df["vendor_comment"] == ""),
                True,
                np.where(
                    df["job_audited"],
                    False,
                    pd.NA
                ),
            )
        else:
            # default behavior
            df["job_correct"] = np.where(
                df["vendor_tag"] == "Approved",
                True,
                np.where(df["job_audited"], False, pd.NA),
            )
    else:
        # Calculating outcome from score
        df['job_correct'] = df['job_score'] >= pass_score
    

    # Let's work on the rubric columns
    for col in rub_col_list:
        s = df[col].fillna("").astype(str).str.strip()
        num = pd.to_numeric(s, errors="coerce")  # numerici validi, il resto diventa NaN

        # condizioni
        is_empty = s == ""
        is_numeric = num.notna()
        is_non_numeric = ~is_numeric & ~is_empty

        # costruisci array finale: default 0
        numeric_int = num.fillna(0).astype(int)
        result = np.select(
            [is_empty, is_numeric, is_non_numeric],
            [0, numeric_int, 1],
            default=0,
        )

        # empty: 0, numeric: int, string: 1
        df[col] = result.astype(int)
    

    
    

    col_list_keep = ['workflow', 'job_date', 'rater_id', 'auditor_id', 'job_id', 'job_correct', 'job_score'] + rub_col_list
    
    df = df[col_list_keep]

    # OUTPUT: ['workflow', 'job_date', 'rater_id', 'auditor_id', 'job_id', 'job_correct', 'job_score'] + [rb_rubric1, rb_rubric2, ...]

    return df



def transform(df, module_info):
    stats = {}
    stats["etl_module"] = "HALO"
    stats["rows_before_transformation"] = len(df)

    # Module config
    mod_config = module_info.get("module_config")
    
    stats["rows_before_transformation"] = len(df)
    df = halo_transform(df, stats, mod_config)
    stats["rows_after_transformation"] = len(df)
    

    # Crea BASE CONFIG per BASE RUBRIC
    rubric_list = mod_config.get("rubric", [])
    default_label_name = mod_config.get("default_label_name", None)
    top_score = mod_config.get("top_score", 100)

    base_config = {
        "rubric": [
            {
                "rubric_name": rubric,
                "rubric_column": f"rb_{rubric}",
                "penalty_score": 5
            }
            for rubric in rubric_list if rubric["rubric_name"] in stats.get("rubric_list", [])
        ],
        "default_label_name": default_label_name,
        "top_score": top_score
    }

    module_info["base_config"] = base_config

    base_df, base_info = brubric(df, base_config)
    
    stats["base_info"] = base_info
    
    return base_df, stats
    