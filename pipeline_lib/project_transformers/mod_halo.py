###################
# Halo Admin Project Transformer
###################

import pandas as pd
import numpy as np
from pipeline_lib.project_transformers import transformer_utils
from pipeline_lib.project_transformers.base_rubric import base_transform as brubric

# --- Logger
import logging
logger = logging.getLogger(__name__)



# Halo Default Columns
HALO_RATER_ID_COL_NAME = "SRT Annotator ID"
HALO_AUDITOR_ID_COL_NAME = "Vendor Auditor ID"
HALO_JOB_ID_COL_NAME = "SRT Job ID"
HALO_JOB_DATE_COL_NAME = "Time (PT)"
HALO_VENDOR_TAG_COL = "Vendor Tag"
HALO_VENDOR_COMMENT_COL = "Vendor Comment"
HALO_VENDOR_SCORE_COL = "Vendor Manual QA Score"


def halo_transform(df, stats, mod_config):
    """
    MOD-CONFIG DICTIONARY FORMAT

    module_config = {
              "info_columns": {
                  "rater_id_column": "Annotator ID",
                  "auditor_id_column": "Vendor Auditor ID",
                  "job_id_column": "SRT Job ID",
                  "submission_date_column": "Time (PT)",
                  "workflow_column": "Rubric Name",
                  "vendor_tag_column": "Vendor Tag",
                  "vendor_comment_column": "Vendor Comment",
                  "manual_score_column": "Vendor Manual QA Score"
              },

              "bottom_score": 80,
              "default_rubric_name": "main_rubric",
              "use_job_score_provided": true,
              "use_job_outcome_provided": false,
              "incorrect_if_commented": true,
              "pass_score": 100,
              
              "rubric": [
                  {
                      "rubric_column" : "Conditions - Incorrect", 
                      "rubric_name" : "conditions_incorrect",
                      "penalty_score": 5
                  },
                  {
                      "rubric_column" : "Can Promotion Be Applied - Incorrect", 
                      "rubric_name" : "can_promotions_be_applied_incorrect",
                      "penalty_score": 5
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
    bottom_score            = int(mod_config.get("bottom_score", 0)) /100
    pass_score              = int(mod_config.get("pass_score", 100)) /100
    default_rubric_name     = mod_config.get("default_rubric_name", "default_rubric")

    
    # Map Info Columns

    rater_id_col            = info.get("rater_id_column", HALO_RATER_ID_COL_NAME)
    auditor_id_col          = info.get("auditor_id_column", HALO_AUDITOR_ID_COL_NAME)
    job_date_col            = info.get("submission_date_column", HALO_JOB_DATE_COL_NAME)
    job_id_col              = info.get("job_id_column", HALO_JOB_ID_COL_NAME)
    workflow_col            = info.get("workflow_column", None)
    vendor_tag_col          = info.get("vendor_tag_column", HALO_VENDOR_TAG_COL)
    vendor_comment_col      = info.get("vendor_comment_column", HALO_VENDOR_COMMENT_COL)
    vendor_score_col        = info.get("vendor_score_column", HALO_VENDOR_SCORE_COL)

    df["wf_col_placeholder"] = "default_workflow"
    
    info_columns_map = {
        rater_id_col            : "rater_id",
        auditor_id_col          : "auditor_id",
        job_date_col            : "job_date",
        job_id_col              : "job_id",
    }
    if workflow_col:
        info_columns_map[workflow_col] = "workflow"
    else:
        info_columns_map["wf_col_placeholder"] = "workflow"

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
        rubric_col = v.get("rubric_column")
        rubric_name = v.get("rubric_name")
        rubric_col_map[rubric_col] = f"rb_{rubric_name}"
    
    rub_valid_map = {src: dst for src, dst in rubric_col_map.items() if src in df.columns} # safely rename only existing columns
    df.rename(columns=rub_valid_map, inplace=True)

    # Saving rubric list
    stats["rubric_list"] = [r.get("rubric_name") for r in rubric_list_items]


    
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
        #Building map rubric_column -> penalty_score
        rubric_items = mod_config.get("rubric", []) or []
        penalty_map = {
            v["rubric_column"]: float(v.get("penalty_score", v.get("penalty", 0))) / 100.0
            for v in rubric_items
            if "rubric_column" in v
        }
        valid_cols = [col for col in rub_col_list if col in penalty_map]
        if valid_cols:
            penalty_series = pd.Series({col: penalty_map[col] for col in valid_cols})
            # moltiplica le colonne per i rispettivi penalty e somma per riga
            total_penalty_series = (df[valid_cols] * penalty_series).sum(axis=1)
        else:
            total_penalty_series = 0

        df["job_score"] = 1 - total_penalty_series
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

    base_config = {
        "rubric": [
            {
                "rubric_name": rubric['rubric_name'],
                "penalty_score": rubric['penalty_score']
            }
            for rubric in rubric_list if rubric["rubric_name"] in stats.get("rubric_list", [])
        ],
    }

    module_info["base_config"] = base_config

    base_df, base_info = brubric(df, base_config)
    
    stats["base_info"] = base_info
    
    return base_df, stats
    