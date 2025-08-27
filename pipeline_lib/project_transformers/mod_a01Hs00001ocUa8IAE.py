# Project Conness - a01Hs00001ocUa8IAE

import pandas as pd
import numpy as np
import re
import unicodedata
from pipeline_lib.project_transformers import transformer_utils as tu

# --- Logger
import logging
logger = logging.getLogger(__name__)

# ADAP Default Columns
ADAP_RATER_ID_COL_NAME = "actor_id"
ADAP_AUDITOR_ID_COL_NAME = "_worker_id"
ADAP_JOB_ID_COL_NAME = "job_id"
ADAP_SUBMISSION_DATE_COL_NAME = "review_ds"
ADAP_WORKFLOW_COL_NAME = "_country"


def sanitize_text(s: object) -> str:
    if pd.isna(s):
        return ""
    s = str(s)
    # Unicode NFC per evitare caratteri composti “strani”
    s = unicodedata.normalize("NFC", s)
    # rimuovi null bytes che corrompono i CSV
    s = s.replace("\x00", "")
    # normalizza newline (pandas quoterà correttamente i \n)
    s = s.replace("\r\n", "\n").replace("\r", "\n")
    # opzionale: rimuovi altri controlli non stampabili (eccetto \n e \t)
    s = re.sub(r"[\x00-\x08\x0B\x0C\x0E-\x1F\x7F]", "", s)
    return s


def adhoc_transform(df, stats):
    quality_methodology = "audit"
    stats["quality_methodology"] = quality_methodology

    # Map columns
    info_column_map = {
        ADAP_RATER_ID_COL_NAME                   : "rater_id",
        ADAP_AUDITOR_ID_COL_NAME                 : "auditor_id",
        ADAP_JOB_ID_COL_NAME                     : "job_id",
        ADAP_SUBMISSION_DATE_COL_NAME            : "job_date",
        ADAP_WORKFLOW_COL_NAME                   : "workflow"
    }
    df.rename(columns=info_column_map, inplace=True)

    label_column_map = {
        "q1_ad_load"                : "is_reviewed",
        "extracted_label"           : "contributor_relevance",
        "q5_feedback"               : "feedback",
    }
    df.rename(columns=label_column_map, inplace=True)

    ## Keep only relevant columns
    #columns_to_keep = list(info_column_map.values()) + list(label_column_map.values())
    #df = df[columns_to_keep].copy()

    # Fix date format and remove rows with incorrect dates
    df["job_date"] = df["job_date"].apply(tu.convert_tricky_date)
    stats["skipped_invalid_datetime"] = int(df["job_date"].isnull().sum())
    df = df[df["job_date"].notnull()].copy()


    # If reason_other is missing, create it
    if "reason_other" not in df.columns:
        df["reason_other"] = "N/A"


    df["contributor_cannot_decide_reason"] = np.where(
        (df["contributor_relevance"] == "can_not_rate") & (df["reason_other"].str.lower() == "n/a"),
        "Other",
        df["reason_other"]
    )

    df["auditor_relevance"] = np.where(
        df["q2_contributor_correct"].str.lower() == "yes",
        df["contributor_relevance"],
        df["q3_choice"]
    )

    df["auditor_cannot_decide_reason"] = np.where(
        (df["q2_contributor_correct"].str.lower() == "yes") &
        ((df["q3_choice"] == "") | (df["q3_choice"].isnull())) &
        ((df["q4_cannot_decide"] == "") | (df["q4_cannot_decide"].isnull())),
        df["contributor_cannot_decide_reason"],
        df["q4_cannot_decide"]
    )

    # Bulk replace
    cols = [
        "contributor_relevance",
        "auditor_relevance",
        "auditor_cannot_decide_reason",
        "contributor_cannot_decide_reason",
    ]
    replace_map = {
        "rating_v1_0_no": "Not Related",
        "rating_v1_1_yes": "Somewhat Related",
        "rating_v1_2_yes": "Relevant",
        "prod_language": "Cannot Decide_Product Language",
        "ad_language": "Cannot Decide_Ad Language",
        "ad_unknown": "Cannot Decide_Ad Unknown",
        "prod_unknown": "Cannot Decide_Product Unknown",
        "prod_issue": "Cannot Decide_Product Issue",
        "ad_issue": "Cannot Decide_Ad Issue",
        "can_not_rate": "Cannot Decide",
        "no_rate_other_reason": "Cannot Decide_Other",
        "Doesn't understand the language the product uses": "Product Language",
        "Doesn't understand the language the ad uses": "Ad Language",
        "Don't know what the ad is promoting": "Ad Unknown",
        "Don't know what the product is promoting": "Product Unknown",
        "Product is not rendering correctly": "Product Issue",
        "Ad is not rendering correctly": "Ad Issue",
        "N/A": "",   # se vuoi vuoto al posto di N/A
    }
    df[cols] = df[cols].replace(replace_map)

    # Filter reviewed jobs
    df = df[df["is_reviewed"].str.lower() == "yes"].copy()
    # At this point, check if the dataframe is empty (after removing not reviewed rows)
    if df.empty:
        logging.warning("DataFrame is empty after filtering. No valid data to process.")
        stats["transform_error"] = "df_empty_after_filtering"
        return pd.DataFrame()
    

    # ID Format check
    df["job_id"] = df["job_id"].apply(tu.id_format_check)
    df["rater_id"] = df["rater_id"].apply(tu.id_format_check)
    df["auditor_id"] = df["auditor_id"].apply(tu.id_format_check)
    mask_cols = ["job_id", "rater_id", "auditor_id"]
    # Count invalid IDs
    mask_invalid_id = df[mask_cols].isnull().any(axis=1)
    stats["skipped_invalid_id"] = int(mask_invalid_id.sum())
    # Remove from df
    df = df[~mask_invalid_id].copy()


    # Fix empty cols
    #cols_to_fix = [c for c in df.columns if c.lower().endswith("cannot_decide_reason")]
    #df[cols_to_fix] = df[cols_to_fix].fillna("").replace("N/A", "")

    # Sanitize feedback
    #df["feedback"] = df["feedback"].apply(sanitize_text)

    # Select columns to keep
    keep_cols = ["workflow", "job_date", "job_id", "rater_id", "auditor_id",
                 "contributor_relevance", "contributor_cannot_decide_reason", #"feedback",
                 "auditor_relevance", "auditor_cannot_decide_reason",
                 ]
    df = df[keep_cols].copy()

    label_rename_map = {
        "contributor_relevance"             : "r_relevance",
        "auditor_relevance"                 : "a_relevance",
        "contributor_cannot_decide_reason"  : "r_cannot_decide_reason",
        "auditor_cannot_decide_reason"      : "a_cannot_decide_reason"
    }
    df.rename(columns=label_rename_map, inplace=True)

    
    # [workflow, job_date, job_id, rater_id, auditor_id] [contributor_label1, contributor_label2, auditor_label1, auditor_label2]

    base_cols = ["workflow", "job_date", "job_id", "rater_id", "auditor_id"]
    all_labels = ["relevance", "cannot_decide_reason"]

    df_long = tu.to_long(df, base_cols, all_labels)
   
    #AUDIT [workflow, job_date, rater_id, auditor_id, job_id] [parent_label] [rater_response, auditor_response]

    df = df_long

    df["is_label_binary"] = False
    df["confusion_type"] = pd.NA

    #[workflow, job_date, rater_id, auditor_id, job_id] [parent_label] [rater_response, auditor_response] [is_label_binary, confusion_type]

    df["is_correct"] = np.where(
        df["parent_label"] == "relevance",
        np.where(
            df["rater_response"] == df["auditor_response"],
            True,
            False
        ),
        np.where(
            df["parent_label"] == "cannot_decide_reason",
            True,
            pd.NA
        )
    ).astype(pd.BooleanDtype)

    df["weight"] = np.where(df["parent_label"] == "cannot_decide_reason", 0, 1)

    #[workflow, job_date, rater_id, auditor_id, job_id] [parent_label] [rater_response, auditor_response] [is_label_binary, confusion_type] [is_correct] [weight]

    # Compile stats
    stats["rows_final"] = len(df)

    return df


def transform(df, project_metadata):
    stats = {}
    stats["etl_module"] = "ADHOC-a01Hs00001ocUa8IAE"
    stats["rows_before_transformation"] = len(df)

    # Module config
    #project_config = project_metadata.get("project_config", {})
    #module_config = project_config.get("module_config", {})

    stats["rows_before_transformation"] = len(df)
    df = adhoc_transform(df, stats)
    stats["rows_after_transformation"] = len(df)

    return df, stats