
# Output to BASE MULTI or AUDIT
# (base-audit) [workflow, job_date, rater_id, auditor_id, job_id] [r_label1, a_label1] [r_label2, a_label2] ...
# (base-multi) [workflow, job_date, rater_id, job_id] [r_label1, r_label2, r_label3] ...

import pandas as pd
import json
import re
from pipeline_lib.project_transformers import transformer_utils as tu

# --- Logger
import logging
logger = logging.getLogger(__name__)


# UQD Default Columns
UQD_RATER_ID_COL_NAME = "actor_id"
UQD_AUDITOR_ID_COL_NAME = "quality_actor_id"
UQD_JOB_ID_COL_NAME = "job_id"
UQD_SUBMISSION_DATE_COL_NAME = "review_ds"
UQD_WORKFLOW_COL_NAME = "queue_name"
UQD_RATER_DECISION_DATA_COL_NAME = "decision_data"
UQD_RATER_EXTRACTED_DECISION_DATA_COL_NAME = "extracted_label"
UQD_AUDITOR_DECISION_DATA_COL_NAME = "quality_decision_data"
UQD_AUDITOR_EXTRACTED_DECISION_DATA_COL_NAME = "quality_extracted_label"


# Flatten values, converting lists/dictionaries into strings
def uqd_format_value(value):
    if isinstance(value, list):
        flattened_value = ",".join(map(str, value))  # Convert list to comma-separated string
        return flattened_value
    elif isinstance(value, dict):
        flattened_value = json.dumps(value, separators=(',', ':'))  # Convert dict to JSON string
        return flattened_value
    return str(value) # Default case


# Extract labels to a list from JSON string
def uqd_extract_labels(json_str, use_extracted):
    try:
        
        if not isinstance(json_str, str) or pd.isna(json_str):
            logger.error("JSON is None or not a string")
            return []

        json_str = json_str.replace("'", '"')

        # Parse first-level JSON
        first_level_json = json.loads(json_str)
        
        if use_extracted:
            logger.debug("Using Extracted Data {json_str}")
            if not isinstance(first_level_json, dict):
                return []
            
            extracted_list = []
            try:
                extracted = [f"{key}::{uqd_format_value(value)}" for key, value in first_level_json.items()]
            except Exception as e1:
                logger.error(f"Error with '::': {e1}")
                return []
            extracted_list.extend(extracted)
            return extracted_list


        if not isinstance(first_level_json, dict):
            logger.error("Top-level JSON is not an object")
            return []
        first_level_key = next(iter(first_level_json), None)

        # Handle different JSON types
        if first_level_key == 'is_rejected':
            # Check if 'response' key exists and contains a valid JSON
            response_str = first_level_json.get("response", None)

            if response_str in (None, "", "null"):
                return []
            
            if not isinstance(response_str, str):
                logger.error("Response key is present but not a string")
                return []

            # Fix JSON formatting in nested response
            nested_str = response_str.replace("'", '"')
            try:
                response_data = json.loads(nested_str)
            except json.JSONDecodeError as e:
                logger.error(f"Nested JSON Decode Error: {e} - {nested_str}")
                return []
            
            if response_data is None:
                return []
            
            if not isinstance(response_data, dict):
                logger.error("Nested response is not an object")
                return []
            
            # Extract entity key
            entity_key = next(iter(response_data), None)
            if not entity_key:
                logger.error("Entity key not found")
                return []
            
            # Retrieve the payload
            entity_data = response_data.get(entity_key, {})
            if not isinstance(entity_data, dict):
                return []
            
            payload = entity_data.get("payload", [])
            if not isinstance(payload, list):
                # Alcuni casi potrebbero fornire un oggetto singolo
                payload = [payload] if payload else []
            
            # Unnest the payload
            extracted_list = []
            for item in payload:
                if not isinstance(item, dict):
                    continue

                values = item.get("values", {})
                #print(f"VALUES {type(values)} {repr(values)}")
                if not isinstance(values, dict):
                    # se values è None o non-dict, salta
                    continue

                try:
                    extracted = [f"{key}::{uqd_format_value(value)}" for key, value in values.items()]
                except Exception as e1:
                    logger.error(f"Error with '::': {e1}")
                    try:
                        fallback_extracted = [f"{key}:{uqd_format_value(value)}" for key, value in values.items()]
                        extracted = [entry.replace(":", "::", 1) for entry in fallback_extracted]
                    except Exception as e2:
                        logger.error(f"Parsing unsuccessful with '::' or ':'.\nError1: {e1}\nError2: {e2}")
                        return []
                extracted_list.extend(extracted)

            #print(f"EXTRACTED LIST ({len(extracted_list)}): {type(extracted_list)} {extracted_list}\n")
            return extracted_list

        elif first_level_key ==  'decision_string':
            labels = first_level_json.get("labels")
            return labels if isinstance(labels, list) else []

        else:
            logger.error("Unknown JSON format")
            return []

    except json.JSONDecodeError as e:
        logger.error(f"JSON Decode Error: {e} - {json_str}")
        return []
    except Exception as e:
        logger.error(f"Unexpected Error: {e} - {json_str}")
        return []



# UQD main transformer function

def uqd_transform(df, stats, mod_config):
    """
    mod_config = {
        "quality_methodology": "multi",
        "use_extracted": True,
        "excluded_labels": [],
        "excluded_queues": [],
        "ignore_missing_auditor_id": False,
        "binary_labels": [
            {
                "label_name": "is_rateable",
                "binary_positive_value": "yes"
            }
        ],
        "label_weights": {
                "label1": 0.0,
                "label2": 0.5,
                "label3": 1,
                "label4": 2
        }
    }
    """

    stats["rows_initial"] = len(df)

    quality_methodology = mod_config.get("quality_methodology", None)
    use_extracted = mod_config.get("use_extracted", False)
    ignore_missing_auditor_id = mod_config.get("ignore_missing_auditor_id", False)
    excluded_list = mod_config.get("excluded_labels", [])
    excluded_queues = mod_config.get("excluded_queues", [])
    binary_labels = mod_config.get("binary_labels", [])
    label_weights = mod_config.get("label_weights", {})

    stats["quality_methodology"] = quality_methodology
    stats["use_extracted"] = use_extracted
    stats["excluded_list"] = excluded_list
    stats["excluded_queues"] = excluded_queues
    stats["binary_labels"] = binary_labels
    stats["label_weights"] = label_weights

    
    needs_auditor = quality_methodology in ("audit", "golden")

    if use_extracted:
        uqd_rater_decision_column = UQD_RATER_EXTRACTED_DECISION_DATA_COL_NAME
        uqd_auditor_decision_column = UQD_AUDITOR_EXTRACTED_DECISION_DATA_COL_NAME
    else:
        uqd_rater_decision_column = UQD_RATER_DECISION_DATA_COL_NAME
        uqd_auditor_decision_column = UQD_AUDITOR_DECISION_DATA_COL_NAME
            
    
    # Map columns
    column_map = {
        UQD_RATER_ID_COL_NAME               : "rater_id",
        UQD_AUDITOR_ID_COL_NAME             : "auditor_id",
        UQD_JOB_ID_COL_NAME                 : "job_id",
        UQD_SUBMISSION_DATE_COL_NAME        : "job_date",
        UQD_WORKFLOW_COL_NAME               : "workflow",
        uqd_rater_decision_column           : "rater_parse_data",
        uqd_auditor_decision_column         : "auditor_parse_data"
    }
   
    df.rename(columns=column_map, inplace=True)

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
        if ignore_missing_auditor_id:
            # replace null/nan with a placeholder
            df["auditor_id"] = df["auditor_id"].fillna("999999999999")
        
    # Count invalid IDs
    mask_invalid_id = df[mask_cols].isnull().any(axis=1)
    stats["skipped_invalid_id"] = int(mask_invalid_id.sum())
    # Remove from df
    df = df[~mask_invalid_id].copy()

    
    # Remove empty rows
    df = df[df["rater_parse_data"].notna() & (df["rater_parse_data"] != '')]
    if needs_auditor:
        df = df[df["auditor_parse_data"].notna() & (df["auditor_parse_data"] != '')]
    
    # Remove excluded queues
    if excluded_queues:
        initial_count = len(df)
        df = df[~df["workflow"].isin(excluded_queues)].copy()
        excluded_count = initial_count - len(df)
        stats["rows_skipped_excluded_queues"] = int(excluded_count)

    
    # Replace semicolon with comma
    df["rater_parse_data"] = df["rater_parse_data"].fillna("").apply(lambda x: x.replace(";", ","))
    if needs_auditor:
        df["auditor_parse_data"] = df["auditor_parse_data"].fillna("").apply(lambda x: x.replace(";", ","))
    
    
    # Parse JSON
    logger.debug("Extracting labels")
    df['rater_labels'] = [uqd_extract_labels(x, use_extracted) for x in df["rater_parse_data"]]
    if needs_auditor:
        df['auditor_labels'] = [uqd_extract_labels(x, use_extracted) for x in df["auditor_parse_data"]]

    #
    # Returns ['key::value', 'key::value', 'key::value']
    #



    # Count excluded json rows
    mask_rater = df["rater_labels"].isna() | df["rater_labels"].astype(str).str.lower().isin(["", "na", "null", "nan"])
    if needs_auditor:
        mask_auditor = df["auditor_labels"].isna() | df["auditor_labels"].astype(str).str.lower().isin(["", "na", "null", "nan"])
        combined_mask = mask_rater | mask_auditor
    else:
        combined_mask = mask_rater
    stats["skipped_invalid_json"] = int(combined_mask.sum())
    # Remove invalid json rows
    df = df[~combined_mask].copy()

    
    # Keep only relevant columns
    cols = ["job_date", "workflow", "job_id", "rater_id", "rater_labels"]
    if needs_auditor:
        cols += ["auditor_id", "auditor_labels"]
    df = df[cols]

    # Drop duplicates
    df.drop_duplicates(subset=["rater_id", "job_id"], keep="last", inplace=True)


    # At this point, check if the dataframe is empty (after removing invalid rows)
    if df.empty:
        logger.warning("DataFrame is empty after filtering. No valid data to process.")
        stats["transform_error"] = "df_empty_after_filtering"
        return pd.DataFrame()

    
    # Expand key values
    rater_labels_pivoted = tu.expand_label_columns(df, "rater_labels", "r", excluded_list)
    # costruisci la parte auditor solo se serve e c'è la colonna
    if needs_auditor and "auditor_labels" in df.columns:
        auditor_labels_pivoted = tu.expand_label_columns(df, "auditor_labels", "a", excluded_list)
    else:
        auditor_labels_pivoted = pd.DataFrame(index=df.index)  # placeholder vuoto

    

    # Label match check
    if needs_auditor:
        # If a key is present in rater_ only, create the corresponding auditor_ column (empty)
        for r_col in rater_labels_pivoted.columns:
            a_col = r_col.replace("r_", "a_", 1)
            if a_col not in auditor_labels_pivoted.columns:
                auditor_labels_pivoted[a_col] = ""
    
        # If a key is present in auditor_ only, remove it
        for a_col in auditor_labels_pivoted.columns:
            r_col = a_col.replace("a_", "r_", 1)
            if r_col not in rater_labels_pivoted.columns:
                auditor_labels_pivoted.drop(columns=[a_col], inplace=True)


    # Concatenate
    result = pd.concat([df, rater_labels_pivoted, auditor_labels_pivoted], axis=1)
    result = result.drop(columns=["rater_labels", "auditor_labels"], errors="ignore")
   

    # Extract labels found and create a list (all_labels)
    def extract_labels(expanded_df, prefix):
        keys = []
        for col in expanded_df.columns:
            if col.startswith(f"{prefix}_"):
                keys.append(col[len(prefix)+1 :])  # rimuove "r_" o "a_"
        return set(keys)

    rater_keys = extract_labels(rater_labels_pivoted, "r")
    auditor_keys = extract_labels(auditor_labels_pivoted, "a") if needs_auditor else set()
    all_labels = sorted(rater_keys.union(auditor_keys))
    stats["label_list"] = all_labels
    
    
    # Rebuild df
    base_cols = ["workflow", "job_date", "rater_id", "job_id"]
    if needs_auditor:
        base_cols.insert(2, "auditor_id")  # order: date, rater_id, auditor_id, job_id

    base_df = df[base_cols].copy()

    # concateno: base + pivotate
    to_concat = [base_df, rater_labels_pivoted]
    if needs_auditor:
        to_concat.append(auditor_labels_pivoted)

    result = pd.concat(to_concat, axis=1)
    result = result.drop(columns=["rater_labels", "auditor_labels"], errors="ignore")

    # [workflow, job_date, rater_id, auditor_id, job_id] [r_label1, r_label2, a_label1, a_label2]

    df_long = tu.to_long(result, base_cols, all_labels)

    #AUDIT [workflow, job_date, rater_id, auditor_id, job_id] [parent_label] [rater_response, auditor_response]
    #MULTI [workflow, job_date, rater_id, auditor_id, job_id] [parent_label] [rater_response]

    df = df_long

    # Add binary flags
    if needs_auditor:
        df = tu.add_binary_flags(df, binary_labels)
        #[workflow, job_date, rater_id, auditor_id, job_id] [parent_label] [rater_response, auditor_response] [is_label_binary, confusion_type]

        df = tu.add_responses_match(df, col_name="is_correct", case_sensitive=False, strip=True)
        #[workflow, job_date, rater_id, auditor_id, job_id] [parent_label] [rater_response, auditor_response] [is_label_binary, confusion_type] [is_correct]


    #AUDIT [workflow, job_date, rater_id, auditor_id, job_id] [parent_label] [rater_response, auditor_response] [is_label_binary, confusion_type] [is_correct]
    #MULTI [workflow, job_date, rater_id, auditor_id, job_id] [parent_label] [rater_response]

    df["weight"] = df["parent_label"].map(label_weights).fillna(1).astype(float)

    #AUDIT [workflow, job_date, rater_id, auditor_id, job_id] [parent_label] [rater_response, auditor_response] [is_label_binary, confusion_type] [is_correct] [weight]
    #MULTI [workflow, job_date, rater_id, auditor_id, job_id] [parent_label] [rater_response] [weight]

    # Compile stats
    stats["rows_final"] = len(df)
  
    return df


def transform(df, project_metadata):
    stats = {}
    stats["etl_module"] = "UQD"
    
    # Module config
    project_config = project_metadata.get("project_config", {})
    module_config = project_config.get("module_config", {})

    stats["rows_before_transformation"] = len(df)
    df = uqd_transform(df, stats, module_config)
    stats["rows_after_transformation"] = len(df) if df is not None else 0

    return df, stats
