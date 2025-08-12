###################
# Generic Project Transformer
###################

import pandas as pd
import numpy as np
from pipeline_lib.project_transformers import transformer_utils as tu


# --- Logger
import logging
logger = logging.getLogger(__name__)



TRUE_TOKENS  = {"1", "true", "yes", "y", "t", "agree", "correct"}
FALSE_TOKENS = {"0", "false", "no", "n", "f", "disagree", "incorrect"}

def as_boolish_series(s: pd.Series, na_as=False) -> pd.Series:
    """
    Converte una Series in booleano True/False in modo vectorizzato.
    
    Args:
        s (pd.Series): colonna da convertire.
        na_as (bool): valore da assegnare a NaN o stringhe vuote.
        
    Returns:
        pd.Series: Series di booleani True/False.
    """
    s_out = s.copy()

    # Gestione NaN e stringhe vuote
    mask_na = s_out.isna() | (s_out.astype(str).str.strip() == "")
    s_out = s_out.astype(str).str.strip().str.lower()

    true_set = TRUE_TOKENS
    false_set = FALSE_TOKENS

    # Inizializziamo il risultato con na_as
    result = pd.Series(na_as, index=s_out.index)

    # True espliciti
    result[s_out.isin(true_set)] = True
    # False espliciti
    result[s_out.isin(false_set)] = False

    # Numerici non zero = True
    num_mask = s_out.str.replace('.', '', 1).str.isnumeric()
    result[num_mask] = s_out[num_mask].astype(float) != 0

    # Applica na_as per i NaN originali o stringhe vuote
    result[mask_na] = na_as

    return result.astype(bool)



def generic_transform(df, stats, mod_config):
    """
    MOD-CONFIG DICTIONARY FORMAT (for Audit projects)

        "module_config": {
               "info_columns": {
                    "rater_id_column": "Annotator ID",
                    "auditor_id_column": "Auditor ID",
                    "job_id_column": "Annotation Job ID",
                    "job_date_column": "Annotation Date And Time"
                },
                "quality_methodology": "audit",
                "labels": [
                    {
                        "label_name": "url_classification",
                        "rater_label_column": "Annotation URL Classification",
                        "auditor_label_column": "Audit URL Classification",
                        "auditor_column_type": "answer",
                    },
                    {
                        "label_name": "url_classification_correct",
                        "auditor_label_column": "KPI: Is URL classified correctly?",
                        "auditor_column_type": "agreement",
                        "empty_as" : False
                    },
                    {
                        "label_name": "title_annotation_correct",
                        "auditor_label_column": "KPI: Is Title annotated incorrectly?",
                        "auditor_column_type": "disagreement",
                        "empty_as": False
                    }
                    
                ]
            }
    """

    """
    MOD-CONFIG DICTIONARY FORMAT (for Multi-pivoted projects)

        "module_config": {
               "info_columns": {
                    "rater_id_column": "actor_id",
                    "job_id_column": "entity_id",
                    "job_date_column": "job_date"
                },
                "quality_methodology": "multi",
                "data_structure": "pivoted",
                "labels": [
                    {
                        "label_name": "url_classification",
                        "rater_label_column": "Annotation URL Classification"
                    },
                    {
                        "label_name": "url_ok",
                        "rater_label_column": "Annotation URL OK"
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
                    "job_date_column": "job_date"
                },
                "quality_methodology": "multi",
                "data_structure": "unpivoted",
                "label_column": "parent_label",
                "rater_response_column": "rater_response",
                "excluded_labels": []
        }
    """

    """
    MOD-CONFIG DICTIONARY FORMAT (for Halo-like projects)

        "module_config": {
               "info_columns": {
                    "rater_id_column": "Annotator ID",
                    "auditor_id_column": "Auditor ID",
                    "job_id_column": "Annotation Job ID",
                    "job_date_column": "Annotation Date And Time"
                },
                "quality_methodology": "outcome",
                "outcome_column": "Is Job Successful?",
                "discard_if_empty": True,
                "positive_outcome": "TRUE",
                "positive_outcome_case_sensitive": False
        }
    """

    info_columns = mod_config.get("info_columns", {})
    quality_methodology = mod_config.get("quality_methodology", None)
    stats["quality_methodology"] = quality_methodology
    needs_auditor = quality_methodology in ["audit", "outcome"]
    
    excluded_labels = mod_config.get("excluded_labels", [])
    stats["excluded_labels"] = excluded_labels
    
    data_structure = mod_config.get("data_structure", None)
    if quality_methodology == "multi":
        if not data_structure:
            stats["transform_error"] = "data_structure_missing"
            return pd.DataFrame()
        stats["data_structure"] = data_structure
    
    

    # MAP INFO COLUMNS
    info_cols_map = {
        info_columns.get('rater_id_column',None)  : "rater_id",
        info_columns.get('job_id_column', None)   : "job_id"
    }

    workflow_col = info_columns.get('workflow_column')
    if not workflow_col:
        df["workflow"] = "default_workflow"
        workflow_col = "workflow"
    info_cols_map[workflow_col] = "workflow"
    
    # può mancare la colonna auditor_id se è un multireview
    if needs_auditor:
        if not info_columns.get('auditor_id_column', None):
            stats["transform_error"] = "auditor_id_column_missing"
            return pd.DataFrame()
        else:
            auditor_col = info_columns.get('auditor_id_column')
            info_cols_map[auditor_col] = "auditor_id"
    
    # può mancare la colonna job_date e in tal caso uso la reporting_week dai metadata
    job_date_col = info_columns.get('job_date_column', None)
    if not job_date_col:
        reporting_week = mod_config.get("reporting_week")
        #print(f"Missing JOB DATE. Forcing reporting week {reporting_week}")
        stats["reporting_week_fallback"] = reporting_week
        df["job_date"] = pd.to_datetime(reporting_week, errors="coerce").strftime("%Y-%m-%d")
        job_date_col = "job_date"
    #else:
    #    df[job_date_col] = df[job_date_col].apply(tu.convert_tricky_date)
    info_cols_map[job_date_col] = "job_date"

    df.rename(columns=info_cols_map, inplace=True)
    base_cols = list(info_cols_map.values())



    # Fix date format and remove rows with incorrect dates
    df["job_date"] = df["job_date"].apply(tu.convert_tricky_date)
    stats["skipped_invalid_datetime"] = int(df["job_date"].isnull().sum())
    df = df[df["job_date"].notnull()].copy()


    # ID Format check
    df["job_id"] = df["job_id"].apply(tu.id_format_check)
    df["rater_id"] = df["rater_id"].apply(tu.id_format_check)
    mask_cols = ["job_id", "rater_id"]
    if needs_auditor:
        df["auditor_id"] = df["auditor_id"].apply(tu.id_format_check)
        mask_cols.append("auditor_id")

    # Count invalid IDs
    mask_invalid_id = df[mask_cols].isnull().any(axis=1)
    stats["skipped_invalid_id"] = int(mask_invalid_id.sum())
    # Remove from df
    df = df[~mask_invalid_id].copy()




    # HANDLE HALO-LIKE (OUTCOME)
    if quality_methodology == "outcome":
        outcome_col = mod_config.get("outcome_column", None)
        discard_if_empty = mod_config.get("discard_if_empty", True)
        positive_outcome = mod_config.get("positive_outcome", True)
        positive_outcome_case_sensitive = mod_config.get("positive_outcome_case_sensitive", False)
        if not outcome_col or outcome_col not in df.columns:
            stats["transform_error"] = "outcome_column_missing"
            return pd.DataFrame()

        if discard_if_empty:
            df = df[df[outcome_col].notna() & (df[outcome_col].astype(str).str.strip() != "")].copy()
        
        if positive_outcome_case_sensitive:
            df["is_correct"] = df[outcome_col].astype(str).str.strip() == str(positive_outcome)
        else:
            df["is_correct"] = df[outcome_col].astype(str).str.strip().str.lower() == str(positive_outcome).lower()

        df = df[base_cols + ["is_correct"]]

        return df


    # HANDLE UNPIVOTED (MULTI)

    if quality_methodology == "multi" and data_structure == "unpivoted":
        unpiv_label_column = mod_config.get("label_column", None)
        unpiv_rater_response_column = mod_config.get("rater_response_column", None)
        # Check if have been declared
        if not unpiv_label_column:
            stats["transform_error"] = "label_column_undeclared"
            return pd.DataFrame()
        if not unpiv_rater_response_column:
            stats["transform_error"] = "rater_response_column_undeclared"
            return pd.DataFrame()
        
        # Check if present in DF
        if unpiv_label_column not in df.columns:
            stats["transform_error"] = "label_column_missing"
            return pd.DataFrame()
        if unpiv_rater_response_column not in df.columns:
            stats["transform_error"] = "rater_response_column_missing"
            return pd.DataFrame()
        
        unpivoted_map = {
            unpiv_label_column: "parent_label",
            unpiv_rater_response_column: "rater_response"
        }
        df.rename(columns=unpivoted_map, inplace=True)

        df = df[base_cols + ["parent_label", "rater_response"]]

        if excluded_labels:
            df_filtered = df[~df["parent_label"].isin(excluded_labels)]
        else:
            df_filtered = df

        return df_filtered



    # HANDLE PIVOTED (MULTI OR AUDIT)

    # Map label columns
    label_dicts = mod_config.get("labels", [])
    label_column_map = {}
    label_flat_list = []
    for v in label_dicts:
        label_name = v['label_name']
        label_flat_list.append(label_name)

        if needs_auditor:
            auditor_label_col = v.get('auditor_label_column', None)
            label_column_map[auditor_label_col] = f"a_{label_name}"

            if v.get('auditor_column_type') in ["agreement", "disagreement"]:
                placeholder_rater_col = f"r_{label_name}"
                df[placeholder_rater_col] = pd.NA  # creates dummy column in the Dataframe
                v['rater_label_column'] = placeholder_rater_col

        rater_label_col = v.get('rater_label_column')
        if not rater_label_col:
            stats["transform_error"] = f"rater_label_column_missing_for_{label_name}"
            return pd.DataFrame()

        label_column_map[rater_label_col] = f"r_{label_name}"

    # Rename columns
    df = df.rename(columns=label_column_map)

    stats["label_list"] = label_flat_list

    # Remove unused columns
    label_cols_ordered = []
    for label_name in label_flat_list:
        r_col = f"r_{label_name}"
        if r_col in df.columns:
            label_cols_ordered.append(r_col)
        
        if needs_auditor:
            a_col = f"a_{label_name}"
            if a_col in df.columns:
                label_cols_ordered.append(a_col)

    final_cols = base_cols + label_cols_ordered
    df = df[final_cols]


    if needs_auditor:
        # per le label che hanno auditor_column_type = agreement/disagreement, devo popolare la rispettiva colonna rater secondo questa logica:
        # se la label è agreement e la risposta "boolish" dell'auditor è True, copiamo nella risposta del rater la risposta (stringa) dell'auditor, altrimenti NA
        # se la label è disagreement e la risposta "boolish" dell'auditor è True, il rater avrà risposta NA, altrimenti copieremo la risposta dell'auditor

        for v in label_dicts:
            label_name = v['label_name']
            r_col = f"r_{label_name}"
            a_col = f"a_{label_name}"

            col_type = v.get('auditor_column_type')
            if col_type in ["agreement", "disagreement"]:
                empty_as = v.get("empty_as", False)
                bool_mask = as_boolish_series(df[a_col], na_as=empty_as)

                if col_type == "agreement":
                    df[r_col] = df[a_col].where(bool_mask, np.nan)
                
                elif col_type == "disagreement":
                    df[r_col] = df[a_col].where(~bool_mask, np.nan)
            

            # outcome column
            out_col = f"o_{label_name}"
            if col_type == "answer":
                # match perfetto tra rater e auditor (incluse stringhe vuote)
                left  = df[r_col].astype("string").fillna("")
                right = df[a_col].astype("string").fillna("")
                df[out_col] = (left == right)

            elif col_type == "agreement":
                # True dove la bool_mask è True, altrimenti False
                df[out_col] = bool_mask.astype(bool)

            elif col_type == "disagreement":
                # False dove la bool_mask è True, True altrimenti
                df[out_col] = (~bool_mask).astype(bool)
        
        # Reorder columns
        label_cols_ordered = []
        for label_name in label_flat_list:
            r_col = f"r_{label_name}"
            a_col = f"a_{label_name}"
            o_col = f"o_{label_name}"
            if r_col in df.columns:
                label_cols_ordered.append(r_col)
            if a_col in df.columns:
                label_cols_ordered.append(a_col)
            if o_col in df.columns:
                label_cols_ordered.append(o_col)

        final_cols = base_cols + label_cols_ordered
        df = df[final_cols]


        # Now we unpivot each label
        df_long = pd.concat(
            [
                df[base_cols].assign(
                    parent_label=label,
                    rater_response=df[f"r_{label}"],
                    auditor_response=df[f"a_{label}"],
                    is_correct=df[f"o_{label}"].astype(bool)
                )
                for label in label_flat_list
            ],
            ignore_index=True
        )

        # Adding binary flags
        df_long["is_label_binary"] = False
        df_long["confusion_type"] = pd.NA
    

    else:
        # No auditor, so we just pivot the labels
        df_long = pd.concat(
            [
                df[base_cols].assign(
                    parent_label=label,
                    rater_response=df[f"r_{label}"]
                )
            for label in label_flat_list],
            ignore_index=True
        )

    df = df_long


    return df




def transform(df, project_metadata):
    stats = {}
    stats["etl_module"] = "GENERIC"
    
    # Module config
    project_config = project_metadata.get("project_config", {})
    module_config = project_config.get("module_config", {})

    stats["rows_before_transformation"] = len(df)
    df = generic_transform(df, stats, module_config)
    stats["rows_after_transformation"] = len(df)
    
    return df, stats
