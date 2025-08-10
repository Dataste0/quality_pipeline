###################
# Generic Project Transformer
###################

import pandas as pd
import numpy as np
from pipeline_lib.project_transformers import transformer_utils
from pipeline_lib.project_transformers.base_audit import base_transform as baudit


# --- Logger
import logging
logger = logging.getLogger(__name__)



# Halo Default Columns
HALO_RATER_ID_COL_NAME = "SRT Annotator ID"
HALO_AUDITOR_ID_COL_NAME = "Vendor Auditor ID"
HALO_JOB_ID_COL_NAME = "SRT Job ID"
HALO_JOB_DATE_COL_NAME = "Time (PT)"
HALO_WORKFLOW_COL_NAME = "label Name"
HALO_VENDOR_TAG_COL = "Vendor Tag"
HALO_VENDOR_COMMENT_COL = "Vendor Comment"
HALO_VENDOR_SCORE_COL = "Vendor Manual QA Score"


# Tipicamente Base Audit con colonne Halo-like
"""
definire singolarmente label e colonne: rater (colonne answer)
auditor (colonne answer/agreement/disagreement)
colonna outcome (definire valore correct/incorrect) se blank è non audited
definire score o penalty
"""

TRUE_TOKENS  = {"1", "true", "yes", "y", "t", "agree", "correct"}
FALSE_TOKENS = {"0", "false", "no", "n", "f", "disagree", "incorrect"}

def _as_boolish(series: pd.Series) -> pd.Series:
    """Converte una serie in boolean (True/False) dove possibile, altrimenti NA."""
    s = series.copy()
    # numerici -> bool (0=False, altri=True)
    is_num = s.apply(lambda x: isinstance(x, (int, float)) and not pd.isna(x))
    s.loc[is_num] = s.loc[is_num].astype(float).astype(int).astype(str)

    # stringhe normalizzate
    s = s.astype(str).str.strip().str.lower()
    s = s.mask(s.isin({"nan", "none", ""}), other=np.nan)

    out = pd.Series(pd.NA, index=series.index, dtype="boolean")
    out = out.mask(s.isin(TRUE_TOKENS), True)
    out = out.mask(s.isin(FALSE_TOKENS), False)
    return out

def generic_audit_transform(df, stats, mod_config):
    """
    MOD-CONFIG DICTIONARY FORMAT (for Audit projects)

        "module_config": {
               "info_columns": {
                    "rater_id_column": "Annotator ID",
                    "auditor_id_column": "Auditor ID",
                    "job_id_column": "Annotation Job ID",
                    "submission_date_column": "Annotation Date And Time"
                },
                "quality_methodology": "audit",
                "labels": [
                    {
                        "label_name": "url_classification",
                        "rater_label_column": "Annotation URL Classification",
                        "auditor_label_column": "Audit URL Classification",
                        "auditor_column_type": "answer",
                        "is_label_binary": true,
                        "label_binary_pos_value": "yes",
                        "weight": "",
                    },
                    {
                        "label_name": "url_classification_correct",
                        "rater_answer_not_recorded": True,
                        "rater_answer_placeholder": "placeholder_response",
                        "auditor_label_column": "KPI: Is URL classified correctly?",
                        "auditor_column_type": "agreement",
                    },
                    {
                        "label_name": "title_annotation_correct",
                        "rater_answer_not_recorded": True,
                        "rater_answer_placeholder": "placeholder_response",
                        "auditor_label_column": "KPI: Is Title annotated correctly?",
                        "auditor_column_type": "agreement",
                    },
                    {
                        "label_name": "main_description_correct",
                        "rater_answer_not_recorded": True,
                        "rater_answer_placeholder": "placeholder_response",
                        "auditor_label_column": "KPI: Is main-description annotated correctly?",
                        "auditor_column_type": "agreement",
                    },
                    {
                        "label_name": "additional_description_correct",
                        "rater_answer_not_recorded": True,
                        "rater_answer_placeholder": "placeholder_response",
                        "auditor_label_column": "KPI: Is additional-description annotated correctly?",
                        "auditor_column_type": "agreement",
                    }
                ]
            }
    """

    """
    MOD-CONFIG DICTIONARY FORMAT (for Multi-unpivoted projects)

        "module_config": {
               "info_columns": {
                    "rater_id_column": "actor_id",
                    "job_id_column": "entity_id",
                    "submission_date_column": "job_date"
                },
                "quality_methodology": "multi",
                "data_structure": "unpivoted",
                "labels": [
                    {
                        "label_name": "url_classification",
                        "rater_label_column": "Annotation URL Classification",
                        "auditor_label_column": "Audit URL Classification",
                        "auditor_column_type": "answer",
                        "is_label_binary": true,
                        "label_binary_pos_value": "yes",
                        "weight": "",
                    },
                    {
                        "label_name": "url_classification_correct",
                        "rater_answer_not_recorded": True,
                        "rater_answer_placeholder": "placeholder_response",
                        "auditor_label_column": "KPI: Is URL classified correctly?",
                        "auditor_column_type": "agreement",
                    },
                    {
                        "label_name": "title_annotation_correct",
                        "rater_answer_not_recorded": True,
                        "rater_answer_placeholder": "placeholder_response",
                        "auditor_label_column": "KPI: Is Title annotated correctly?",
                        "auditor_column_type": "agreement",
                    },
                    {
                        "label_name": "main_description_correct",
                        "rater_answer_not_recorded": True,
                        "rater_answer_placeholder": "placeholder_response",
                        "auditor_label_column": "KPI: Is main-description annotated correctly?",
                        "auditor_column_type": "agreement",
                    },
                    {
                        "label_name": "additional_description_correct",
                        "rater_answer_not_recorded": True,
                        "rater_answer_placeholder": "placeholder_response",
                        "auditor_label_column": "KPI: Is additional-description annotated correctly?",
                        "auditor_column_type": "agreement",
                    }
                ]
            }
    """
    
    info_columns = mod_config.get("info_columns", {})

    info_cols_map = {
        info_columns.get('rater_id_column',     HALO_RATER_ID_COL_NAME)         : "rater_id",
        info_columns.get('auditor_id_column',   HALO_AUDITOR_ID_COL_NAME)       : "auditor_id",
        info_columns.get('job_id_column',       HALO_JOB_ID_COL_NAME)           : "job_id",
        info_columns.get('submission_date_column', HALO_JOB_DATE_COL_NAME)      : "job_date"
    }
    if info_columns.get('workflow_column'):
        info_cols_map[info_columns['workflow_column']] = "workflow"
    else:
        df["workflow"] = "default_workflow"
    df.rename(columns=info_cols_map, inplace=True)


    # Map label columns
    label_dicts = mod_config.get("labels", [])
    label_column_map = {}
    label_flat_list = []
    for v in label_dicts:
        label_name = v['label_name']
        label_flat_list.append(label_name)

        if v.get('rater_answer_not_recorded'):
            placeholder_col = f"rater_{label_name}" # dummy column
            placeholder_val = pd.NA         
            df[placeholder_col] = placeholder_val
            auditor_col = f"auditor_{label_name}"
        else:
            rater_label_col = v['rater_label_column']
            label_column_map[rater_label_col] = f"rater_{label_name}"
        auditor_label_col = v['auditor_label_column']
        label_column_map[auditor_label_col] = f"auditor_{label_name}"
    df = df.rename(columns=label_column_map)

    stats["label_list"] = label_flat_list

    # Required columns
    info_columns = ["workflow", "job_date", "rater_id", "auditor_id", "job_id"]
    label_cols_renamed = [
        col
        for l in label_flat_list
        for col in (f"rater_{l}", f"auditor_{l}")
    ]

    required_df_cols = info_columns + label_cols_renamed
    df = df.loc[:, [col for col in required_df_cols if col in df.columns]]

    # Fix date format and remove rows with incorrect dates
    df["job_date"] = df["job_date"].apply(transformer_utils.convert_tricky_date)
    stats["skipped_invalid_datetime"] = int(df["job_date"].isnull().sum())
    df = df[df["job_date"].notnull()].copy()


    # ["workflow", "job_date", "rater_id", "job_id"] [is_rateable|rater, is_rateable|auditor] [withhold|rater, withhold|auditor] ...

    return df



def transform(df, module_info):
    stats = {}
    stats["etl_module"] = "GENERIC"
    stats["rows_before_transformation"] = len(df)

    # Module config
    mod_config = module_info.get("module_config")
    
    stats["rows_before_transformation"] = len(df)
    df = generic_audit_transform(df, stats, mod_config)
    stats["rows_after_transformation"] = len(df)
        

    # Crea BASE CONFIG
    label_list = mod_config.get("labels", []) # Ottiene labels da module_config
    
    base_config = {
        "labels": [
            {
                "label_name": item.get('label_name'),
                "auditor_column_type": item.get('auditor_column_type'),
                "is_label_binary": item.get('is_label_binary', False),
                "label_binary_pos_value": item.get('label_binary_pos_value', None),
                "weight": item.get('weight', 1)
            }
            for item in label_list
        ],
    }


    module_info["base_config"] = base_config

    base_df, base_info = baudit(df, base_config)
    
    stats["base_info"] = base_info
    
    return base_df, stats
    