
# Output to BASE MULTI or AUDIT
# (base-audit) [workflow, job_date, rater_id, auditor_id, job_id] [r_label1, a_label1] [r_label2, a_label2] ...
# (base-multi) [workflow, job_date, rater_id, job_id] [r_label1, r_label2, r_label3] ...

import pandas as pd
import json
import re
from pipeline_lib.project_transformers import transformer_utils
from pipeline_lib.project_transformers.base_audit import base_transform as baudit
from pipeline_lib.project_transformers.base_multi import base_transform as bmulti

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
def UQD_format_value(value):
    if isinstance(value, list):
        flattened_value = ",".join(map(str, value))  # Convert list to comma-separated string
        return flattened_value
    elif isinstance(value, dict):
        flattened_value = json.dumps(value, separators=(',', ':'))  # Convert dict to JSON string
        return flattened_value
    return str(value) # Default case


# Extract labels to a list from JSON string
def UQD_extract_labels(json_str, use_extracted):
    try:
        #print(f"RAW {type(json_str)} {repr(json_str)}")
        if not isinstance(json_str, str) or pd.isna(json_str):
            logging.error("JSON is None or not a string")
            return []

        json_str = json_str.replace("'", '"')

        # Parse first-level JSON
        first_level_json = json.loads(json_str)
        #print(f"PARSED {type(first_level_json)} {repr(first_level_json)}")

        if use_extracted:
            logging.debug("Using Extracted Data {json_str}")
            extracted_list = []
            try:
                extracted = [f"{key}::{UQD_format_value(value)}" for key, value in first_level_json.items()]
            except Exception as e1:
                logging.error(f"Error with '::': {e1}")
                return []
            extracted_list.extend(extracted)
            #print(f"ALREADY EXTRACTED LABELS ({len(extracted_list)}): {type(extracted_list)} {extracted_list}\n")
            return extracted_list


        first_level_key = next(iter(first_level_json), None)
        #print(f"FIRST LEVEL KEY: {type(first_level_key)} {first_level_key}")

        # Handle different JSON types
        if first_level_key == 'is_rejected':
            # Check if 'response' key exists and contains a valid JSON
            response_str = first_level_json.get("response", None)
            if not response_str or not isinstance(response_str, str):
                logging.error("Response key is missing or not a string")
                return []

            # Fix JSON formatting in nested response
            response_str = response_str.replace("'", '"')
            response_data = json.loads(response_str)
            #print(f"RESPONSE {type(response_data)} {repr(response_data)}")

            # Extract entity key
            entity_key = next(iter(response_data), None)
            if not entity_key:
                logging.error("Entity key not found")
                return []
            #print(f"ENTITY KEY {type(entity_key)} {repr(entity_key)}")

            # Retrieve the payload
            entity_data = response_data.get(entity_key, {})
            payload = entity_data.get("payload", [])
            #print(f"PAYLOAD {type(payload)} {repr(payload)}")

            # Unnest the payload
            extracted_list = []
            for item in payload:
                values = item.get("values", {})
                #print(f"VALUES {type(values)} {repr(values)}")

                try:
                    extracted = [f"{key}::{UQD_format_value(value)}" for key, value in values.items()]
                except Exception as e1:
                    logging.error(f"Error with '::': {e1}")
                    try:
                        fallback_extracted = [f"{key}:{UQD_format_value(value)}" for key, value in values.items()]
                        extracted = [entry.replace(":", "::", 1) for entry in fallback_extracted]
                    except Exception as e2:
                        logging.error(f"Parsing unsuccessful with '::' or ':'.\nError1: {e1}\nError2: {e2}")
                        return []

                extracted_list.extend(extracted)

            #print(f"EXTRACTED LIST ({len(extracted_list)}): {type(extracted_list)} {extracted_list}\n")
            return extracted_list

        elif first_level_key ==  'decision_string':
            labels = first_level_json.get("labels", None)
            #print(f"EXTRACTED LABELS ({len(labels)}): {type(labels)} {labels}\n")
            return labels

        else:
            logging.error("Unknown JSON format")
            return []

    except json.JSONDecodeError as e:
        logging.error(f"JSON Decode Error: {e} - {json_str}")
        return []
    except Exception as e:
        logging.error(f"Unexpected Error: {e}")
        return []



def expand_label_columns(df, label_col, prefix, excluded_list=None):
    excluded_set = set(x.strip().lower() for x in (excluded_list or []))
    tmp = df[[label_col]].copy()
    tmp[label_col] = tmp[label_col].apply(lambda x: x if isinstance(x, list) else [])
    exploded = tmp.explode(label_col).reset_index()  # mantiene indice originale

    def split_kv(s):
        if not isinstance(s, str) or "::" not in s:
            return pd.Series({f"{prefix}_key": None, f"{prefix}_value": None})
        k, v = s.split("::", 1)
        k_clean = k.strip().lower()
        return pd.Series({f"{prefix}_key": k_clean, f"{prefix}_value": v})

    kv = exploded[label_col].apply(split_kv)
    exploded = pd.concat([exploded, kv], axis=1)

    # scarta le key escluse (senza prefisso: qui è solo 'quality', 'speed', ecc.)
    exploded = exploded[~exploded[f"{prefix}_key"].isin(excluded_set)]

    pivoted = (
        exploded
        .dropna(subset=[f"{prefix}_key"])
        .pivot_table(
            index=exploded["index"],
            columns=f"{prefix}_key",
            values=f"{prefix}_value",
            aggfunc=lambda x: x.iloc[0] if len(x) else None,
        )
    )
    pivoted.columns = [f"{prefix}_{col}" for col in pivoted.columns]
    pivoted = pivoted.reindex(df.index, fill_value=None)
    return pivoted






def UQD_transform(df, stats, mod_config):
    """
    mod_config = {
        "quality_methodology": "multi",
        "use_extracted": True,
        "excluded_labels": [],
        "binary_labels": [
            {
                "label_name": "is_rateable",
                "binary_positive_value": "yes"
            }
        ]
    }
    """

    stats["rows_initial"] = len(df)

    quality_methodology = mod_config.get("quality_methodology", None)
    use_extracted = mod_config.get("use_extracted", False)
    excluded_list = mod_config.get("excluded_labels", [])

    needs_auditor = quality_methodology in ("audit", "golden")
    stats["quality_methodology"] = quality_methodology

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
    df["job_date"] = df["job_date"].apply(transformer_utils.convert_tricky_date)
    stats["skipped_invalid_datetime"] = int(df["job_date"].isnull().sum())
    df = df[df["job_date"].notnull()].copy()
    
    # Fix date format and remove rows with incorrect dates
    df["job_date"] = df["job_date"].apply(transformer_utils.convert_tricky_date)
    stats["skipped_invalid_datetime"] = int(df["job_date"].isnull().sum())
    df = df[df["job_date"].notnull()].copy()

    # ID Format check
    df["job_id"] = df["job_id"].apply(transformer_utils.id_format_check)
    df["rater_id"] = df["rater_id"].apply(transformer_utils.id_format_check)
    mask_cols = ["job_id", "rater_id"]
    if needs_auditor:
        if quality_methodology == 'golden':
            df["auditor_id"] = 'golden_set'
        else:
            df["auditor_id"] = df["auditor_id"].apply(transformer_utils.id_format_check)
        mask_cols.append("auditor_id")

    # Count invalid IDs
    mask_cols = []
    mask_invalid_id = df[mask_cols].isnull().any(axis=1)
    stats["skipped_invalid_id"] = int(mask_invalid_id.sum())
    # Remove from df
    df = df[~mask_invalid_id].copy()


    # Remove empty rows
    df = df[df["rater_parse_data"].notna() & (df["rater_parse_data"] != '')]
    if needs_auditor:
        df = df[df["auditor_parse_data"].notna() & (df["auditor_parse_data"] != '')]


    # Replace semicolon with comma
    df["rater_parse_data"] = df["rater_parse_data"].fillna("").apply(lambda x: x.replace(";", ","))
    if needs_auditor:
        df["auditor_parse_data"] = df["auditor_parse_data"].fillna("").apply(lambda x: x.replace(";", ","))


    # Parse JSON
    logging.debug("Extracting labels UQD_audit")
    df['rater_labels'] = [UQD_extract_labels(x, use_extracted) for x in df["rater_parse_data"]]
    if needs_auditor:
        df['auditor_labels'] = [UQD_extract_labels(x, use_extracted) for x in df["auditor_parse_data"]]

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
        logging.warning("DataFrame is empty after filtering. No valid data to process.")
        stats["transform_error"] = "df_empty_after_filtering"
        return pd.DataFrame()

    
    # Expand key values
    rater_labels_pivoted = expand_label_columns(df, "rater_labels", "rater", excluded_list)
    # costruisci la parte auditor solo se serve e c'è la colonna
    if needs_auditor and "auditor_labels" in df.columns:
        auditor_labels_pivoted = expand_label_columns(df, "auditor_labels", "auditor", excluded_list)
    else:
        auditor_labels_pivoted = pd.DataFrame(index=df.index)  # placeholder vuoto

    

    # Label match check
    if needs_auditor:
        # If a key is present in rater_ only, create the corresponding auditor_ column (empty)
        for r_col in rater_labels_pivoted.columns:
            a_col = r_col.replace("rater_", "auditor_", 1)
            if a_col not in auditor_labels_pivoted.columns:
                auditor_labels_pivoted[a_col] = ""
    
        # If a key is present in auditor_ only, remove it
        for a_col in auditor_labels_pivoted.columns:
            r_col = a_col.replace("auditor_", "rater_", 1)
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


    rater_keys = extract_labels(rater_labels_pivoted, "rater")
    auditor_keys = extract_labels(auditor_labels_pivoted, "auditor") if needs_auditor else set()
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


    # Compile stats
    stats["rows_final"] = len(result)
  
    return result


def transform(df, module_info):
    stats = {}
    stats["etl_module"] = "UQD"

    #print(f"MODULE INFO: {module_info}")

    # Module config
    mod_config = module_info.get("module_config")
    quality_methodology = mod_config.get("quality_methodology")

    logging.info(f"Transforming UQD data")
    
    stats["rows_before_transformation"] = len(df)
    df = UQD_transform(df, stats, mod_config)
    stats["rows_after_transformation"] = len(df)

    logging.info(f"Transformed UQD data: {len(df)} rows")
    
    #df = transformer_utils.enrich_dataframe_with_metadata(df, metadata)
    #logging.info(f"Enriched UQD data with metadata")



    # UQD audit : base audit
    # UQD golden: base audit
    # UQD multi : base multi
    
    # Crea BASE CONFIG per BASE AUDIT
    needs_auditor = True if quality_methodology == 'audit' or quality_methodology == 'golden' else False

    binary_labels_dict = {
        v["label_name"] : v["binary_positive_value"]
        for v in mod_config.get("binary_labels", [])
    }


    module_info["base_config"] = {
        "labels": [
            {
                "label_name": label,
                "is_label_binary": label in binary_labels_dict,
                "label_binary_pos_value": binary_labels_dict.get(label),
                "weight": 1,
                "auditor_column_type": "answer" if needs_auditor else None,
            }
            for label in stats.get("label_list", [])
        ]
    }

    if df.empty:
        return pd.DataFrame(), stats

    base_config = module_info.get("base_config", {})
    if quality_methodology == 'multi':
        base_df, base_info = bmulti(df, base_config)
    else:
        base_df, base_info = baudit(df, base_config)
    
    stats["base_info"] = base_info
    
    return base_df, stats