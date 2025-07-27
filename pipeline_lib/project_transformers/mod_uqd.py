import pandas as pd
import json
import re
import logging
from pipeline_lib.project_transformers import transformer_utils

# --- Setup logger
logger = logging.getLogger('pipeline.transform_modules')


# Actor ID/Job ID check
def UQD_int_number_check(val):
    if val is None or pd.isna(val) or val == "":
        return ""
    
    int_number_regex = re.compile(r'^\d+$')
    if isinstance(val, str) and int_number_regex.fullmatch(val) and 'e' not in val.lower():
        return val
    
    return 'pipeline_error::invalid_number_format'


def UQD_filter_out_invalid_id_rows(df):
    initial_row_count = len(df)
    logging.debug(f"Filtering out rows with invalid IDs - Initial row count: {initial_row_count}")
    
    filtered_df = df[
        (df['actor_id'] != 'pipeline_error::invalid_number_format') &
        (df['job_id'] != 'pipeline_error::invalid_number_format')
    ].copy()

    final_row_count = len(filtered_df)
    logging.debug(f"Filtering out rows with invalid IDs - Final row count: {final_row_count}")
    skipped = initial_row_count - final_row_count
    
    return filtered_df, skipped


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
            logging.debug("Using Extracted Data")
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

# Filter out excluded labels
def UQD_filter_labels(label_list, excluded_list):
    if not isinstance(label_list, list):
        #print(f"Label List: {label_list}")
        print("ERROR - Filter Labels: expected a list of labels")
        return ['pipeline_error::invalid_json_format']

    return [item for item in label_list if item.split("::")[0] not in excluded_list]

# Compute confusion type for rater_labels
def UQD_compute_confusion_type(rater_labels, auditor_labels, binary_labels):
    if not isinstance(rater_labels, list) or not isinstance(auditor_labels, list):
        logging.error("UQD_compute_confusion_type - Format error: expected list")
        return []

    updated_labels = []
    for label in rater_labels:
        key_value = label.split("::")
        if len(key_value) == 2:
            key, value = key_value

            if key in binary_labels:
                pos_val = binary_labels[key]["positive_value"].lower()

                auditor_value = next((aud_label.split("::")[1] for aud_label in auditor_labels if aud_label.startswith(f"{key}::")), None)

                # Compute confusion type
                rater_lower = value.lower()
                auditor_lower = auditor_value.lower() if auditor_value else ""

                if pos_val in auditor_lower:
                    confusion_type = "TP" if pos_val in rater_lower else "FN"
                else:
                    confusion_type = "FP" if pos_val in rater_lower else "TN"

                # Append confusion type
                updated_labels.append(f"{key}::{value}|{confusion_type}")
            else:
                updated_labels.append(label)  # Keep as is if not in binary_labels
        else:
            logging.error("UQD_compute_confusion_type - Incorrect format")
            return []
    return updated_labels

# Drop duplicates
def UQD_drop_dupes(df, grouped_columns, aggregated_columns):
    # Mask to exclude rows with NaN or empty strings in - at least - 1 of the selected_columns
    mask = df[aggregated_columns].notna().all(axis=1) & (df[aggregated_columns] != "").all(axis=1)
    df_clean = df[mask]
    # Removes dupes
    df_deduped = df_clean.drop_duplicates(subset=grouped_columns, keep='first')
    return df_deduped[grouped_columns + aggregated_columns]


# Explode labels
def UQD_unpivot_labels(df, role):
    labels_col = f"{role}_labels"

    # Explode the list of labels into separate rows; drop rows with null labels
    df_exploded = df.explode(labels_col).dropna(subset=[labels_col])

    # Safe split function: always returns tuple of length 2
    def safe_split_label(x):
        try:
            if pd.isna(x) or not isinstance(x, str) or x.strip() == "":
                return (None, None)
            parts = x.split("::", 1)
            return (parts[0], parts[1]) if len(parts) == 2 else (parts[0], None)
        except Exception:
            return (None, None)

    def safe_split_confusion(x):
        try:
            if pd.isna(x) or not isinstance(x, str) or x.strip() == "":
                return (None, None)
            parts = x.split("|", 1)
            return (parts[0], parts[1]) if len(parts) == 2 else (parts[0], None)
        except Exception:
            return (None, None)

    # Use .apply and convert to DataFrame explicitly
    label_split = df_exploded[labels_col].apply(safe_split_label)
    df_exploded["label_name"] = label_split.apply(lambda x: x[0])
    df_exploded["label_response"] = label_split.apply(lambda x: x[1])

    confusion_split = df_exploded["label_response"].apply(safe_split_confusion)
    df_exploded["label_response"] = confusion_split.apply(lambda x: x[0])
    df_exploded["confusion_type"] = confusion_split.apply(lambda x: x[1])

    df_exploded["confusion_type"] = df_exploded["confusion_type"].fillna("")

    df_exploded["is_audit"] = 1 if role == "auditor" else 0
    df_exploded["source_of_truth"] = 1 if role == "golden" else 0

    df_exploded = df_exploded.drop(columns=[labels_col])

    return df_exploded

# Bulk rename columns
def UQD_bulk_rename_to_universal_format(df):
    rename_dict = {
        "rater_id"            : "actor_id",
        "auditor_id"          : "actor_id",
        "quality_actor_id"    : "actor_id",

        "queue_name"          : "workflow",
        "routing_name"        : "workflow",

        "review_ds"           : "submission_date",
        "sample_ds"           : "submission_date",

        "entity_id"           : "job_id",

        "label_name"          : "parent_label",
        "label_response"      : "response_data",
        "confusion_type"      : "recall_precision"
    }
    existing_renames = {k: v for k, v in rename_dict.items() if k in df.columns}
    return df.rename(columns=existing_renames)


def UQD_audit_transform(df, binary_labels, excluded_labels, use_extracted):
    info_audit_rows_initial = len(df)

    # Fix date format and remove rows with incorrect dates
    df['review_ds'] = df['review_ds'].apply(transformer_utils.convert_tricky_date)
    info_audit_skipped_invalid_datetime = df['review_ds'].isnull().sum()
    df = df[df['review_ds'].notnull()].copy()

    if use_extracted:
        RATER_DECISION_COLUMN = "extracted_label"
        AUDITOR_DECISION_COLUMN = "quality_extracted_label"
    else:
        RATER_DECISION_COLUMN = "decision_data"
        AUDITOR_DECISION_COLUMN = "quality_decision_data"
    
    # ID Format check
    df['actor_id'] = df['actor_id'].apply(transformer_utils.id_format_check)
    df['quality_actor_id'] = df['quality_actor_id'].apply(transformer_utils.id_format_check)
    df['entity_id'] = df['entity_id'].apply(transformer_utils.id_format_check)
    # Count invalid IDs
    mask_invalid_id = df[['actor_id', 'quality_actor_id', 'entity_id']].isnull().any(axis=1)
    info_audit_skipped_invalid_id = mask_invalid_id.sum()
    # Remove from df
    df = df[~mask_invalid_id].copy()

    # Remove empty rows
    df = df[df[RATER_DECISION_COLUMN].notna() & (df[RATER_DECISION_COLUMN] != '')]
    df = df[df[AUDITOR_DECISION_COLUMN].notna() & (df[AUDITOR_DECISION_COLUMN] != '')]
    # Replace semicolon with comma
    df[RATER_DECISION_COLUMN] = df[RATER_DECISION_COLUMN].fillna("").apply(lambda x: x.replace(";", ","))
    df[AUDITOR_DECISION_COLUMN] = df[AUDITOR_DECISION_COLUMN].fillna("").apply(lambda x: x.replace(";", ","))
    # Parse JSON
    logging.debug("Extracting labels UQD_audit")
    df['rater_labels'] = [UQD_extract_labels(x, use_extracted) for x in df[RATER_DECISION_COLUMN]]
    df['auditor_labels'] = [UQD_extract_labels(x, use_extracted) for x in df[AUDITOR_DECISION_COLUMN]]

    # Count excluded json rows
    mask_rater = df['rater_labels'].isna() | df['rater_labels'].astype(str).str.lower().isin(['', 'na', 'null', 'nan'])
    mask_auditor = df['auditor_labels'].isna() | df['auditor_labels'].astype(str).str.lower().isin(['', 'na', 'null', 'nan'])
    info_audit_skipped_invalid_json = (mask_rater | mask_auditor).sum()
    # Remove invalid json rows
    df = df[~(mask_rater | mask_auditor)].copy()

    # Filter out excluded labels
    logging.debug("Filtering labels UQD_audit")
    df['rater_labels'] = df['rater_labels'].apply(UQD_filter_labels, args=(excluded_labels,))
    df['auditor_labels'] = df['auditor_labels'].apply(UQD_filter_labels, args=(excluded_labels,))
    
    # Compute confusion_type and concatenate it to rater binary labels in rater_labels column
    df["rater_labels"] = [
        UQD_compute_confusion_type(rater, auditor, binary_labels)
        for rater, auditor in zip(df["rater_labels"], df["auditor_labels"])
    ]

    # Drop duplicates
    df_rater = UQD_drop_dupes(df, grouped_columns=["review_ds", "queue_name", "entity_id", "actor_id"], aggregated_columns=["rater_labels"])
    df_auditor = UQD_drop_dupes(df, grouped_columns=["queue_name", "entity_id"], aggregated_columns=["quality_actor_id", "auditor_labels", "review_ds"])
    # Unpivot
    df_rater = UQD_unpivot_labels(df_rater,'rater')
    df_auditor = UQD_unpivot_labels(df_auditor,'auditor')

    # Bulk rename to Universal format
    df_rater = UQD_bulk_rename_to_universal_format(df_rater)
    df_auditor = UQD_bulk_rename_to_universal_format(df_auditor)
    # Combine dataframes
    df_combined = pd.concat([df_rater, df_auditor], ignore_index=True, sort=False).fillna("")
    #print(f"AUDIT DATAFRAME: {df_combined.columns}")

    info_audit_rows_final = len(df_combined)
    info_audit = {
        #"methodology": "audit",
        "rows_initial": int(info_audit_rows_initial),
        "rows_final": int(info_audit_rows_final),
        "skipped_invalid_datetime": int(info_audit_skipped_invalid_datetime),
        "skipped_invalid_json": int(info_audit_skipped_invalid_json),
        "skipped_invalid_id": int(info_audit_skipped_invalid_id)
    }
    return df_combined, info_audit

def UQD_golden_transform(df, binary_labels, excluded_labels, use_extracted):
    info_golden_rows_initial = len(df)

    # Fix date format and remove rows with incorrect dates
    df['review_ds'] = df['review_ds'].apply(transformer_utils.convert_tricky_date)
    info_golden_skipped_invalid_datetime = df['review_ds'].isnull().sum()
    df = df[df['review_ds'].notnull()].copy()

    if use_extracted:
        RATER_DECISION_COLUMN = "extracted_label"
        GOLDEN_DECISION_COLUMN = "quality_extracted_label"
    else:
        RATER_DECISION_COLUMN = "decision_data"
        GOLDEN_DECISION_COLUMN = "quality_decision_data"
    
    # ID Format check
    df['actor_id'] = df['actor_id'].apply(transformer_utils.id_format_check)
    #df['quality_actor_id'] = df['quality_actor_id'].apply(transformer_utils.id_format_check)
    df['entity_id'] = df['entity_id'].apply(transformer_utils.id_format_check)
    # Count invalid IDs
    #mask_invalid_id = df[['actor_id', 'quality_actor_id', 'entity_id']].isnull().any(axis=1)
    mask_invalid_id = df[['actor_id', 'entity_id']].isnull().any(axis=1)
    info_golden_skipped_invalid_id = mask_invalid_id.sum()
    # Remove from df
    df = df[~mask_invalid_id].copy()


    # Remove empty rows
    df = df[df[RATER_DECISION_COLUMN].notna() & (df[RATER_DECISION_COLUMN] != '')]
    df = df[df[GOLDEN_DECISION_COLUMN].notna() & (df[GOLDEN_DECISION_COLUMN] != '')]
    # Replace semicolon with comma
    df[RATER_DECISION_COLUMN] = df[RATER_DECISION_COLUMN].fillna("").apply(lambda x: x.replace(";", ","))
    df[GOLDEN_DECISION_COLUMN] = df[GOLDEN_DECISION_COLUMN].fillna("").apply(lambda x: x.replace(";", ","))
    # Parse JSON
    df['rater_labels'] = [UQD_extract_labels(x, use_extracted) for x in df[RATER_DECISION_COLUMN]]
    df['golden_labels'] = [UQD_extract_labels(x, use_extracted) for x in df[GOLDEN_DECISION_COLUMN]]

    # Count excluded json rows
    mask_rater = df['rater_labels'].isna() | df['rater_labels'].astype(str).str.lower().isin(['', 'na', 'null', 'nan'])
    mask_golden = df['golden_labels'].isna() | df['golden_labels'].astype(str).str.lower().isin(['', 'na', 'null', 'nan'])
    info_golden_skipped_invalid_json = (mask_rater | mask_golden).sum()
    # Remove invalid json rows
    df = df[~(mask_rater | mask_golden)].copy()


    # Filter out excluded labels
    df['rater_labels'] = df['rater_labels'].apply(UQD_filter_labels, args=(excluded_labels,))
    df['golden_labels'] = df['golden_labels'].apply(UQD_filter_labels, args=(excluded_labels,))
    # Compute confusion_type and concatenate it to rater binary labels in rater_labels column
    df["rater_labels"] = [
        UQD_compute_confusion_type(rater, golden, binary_labels)
        for rater, golden in zip(df["rater_labels"], df["golden_labels"])
    ]


    # Drop duplicates
    df_rater = UQD_drop_dupes(df, grouped_columns=["review_ds", "queue_name", "entity_id", "actor_id"], aggregated_columns=["rater_labels"])
    df_golden = UQD_drop_dupes(df, grouped_columns=["queue_name", "entity_id"], aggregated_columns=["quality_actor_id", "golden_labels", "review_ds"])
    # Unpivot
    df_rater = UQD_unpivot_labels(df_rater,'rater')
    df_golden = UQD_unpivot_labels(df_golden,'golden')

    # Bulk rename to Universal format
    df_rater = UQD_bulk_rename_to_universal_format(df_rater)
    df_golden = UQD_bulk_rename_to_universal_format(df_golden)
    # Combine dataframes
    df_combined = pd.concat([df_rater, df_golden], ignore_index=True, sort=False).fillna("")
    #print(f"GOLDEN DATAFRAME: {df_combined.columns}")

    info_golden_rows_final = len(df_combined)
    info_golden = {
        #"methodology": "golden",
        "rows_initial": int(info_golden_rows_initial),
        "rows_final": int(info_golden_rows_final),
        "skipped_invalid_datetime": int(info_golden_skipped_invalid_datetime),
        "skipped_invalid_json": int(info_golden_skipped_invalid_json),
        "skipped_invalid_id": int(info_golden_skipped_invalid_id)
    }
    return df_combined, info_golden


def UQD_multi_transform(df, binary_labels, excluded_labels, use_extracted):
    info_multi_rows_initial = len(df)
    #df.to_csv('test-multi1.csv')

    # Fix date format and remove rows with incorrect dates
    df['review_ds'] = df['review_ds'].apply(transformer_utils.convert_tricky_date)
    info_multi_skipped_invalid_datetime = df['review_ds'].isnull().sum()
    df = df[df['review_ds'].notnull()].copy()


    if use_extracted:
        RATER_DECISION_COLUMN = "extracted_label"
        #AUDITOR_DECISION_COLUMN = "quality_extracted_label"
    else:
        RATER_DECISION_COLUMN = "decision_data"
        #AUDITOR_DECISION_COLUMN = "quality_decision_data"

    # ID Format check
    df['actor_id'] = df['actor_id'].apply(transformer_utils.id_format_check)
    df['entity_id'] = df['entity_id'].apply(transformer_utils.id_format_check)
    # Count invalid IDs
    mask_invalid_id = df[['actor_id', 'entity_id']].isnull().any(axis=1)
    info_multi_skipped_invalid_id = mask_invalid_id.sum()
    # Remove from df
    df = df[~mask_invalid_id].copy()

    #df.to_csv('test-multi2.csv')

    # Remove empty rows
    df = df[df[RATER_DECISION_COLUMN].notna() & (df[RATER_DECISION_COLUMN] != '')]
    # Replace semicolon with comma
    df[RATER_DECISION_COLUMN] = df[RATER_DECISION_COLUMN].fillna("").apply(lambda x: x.replace(";", ","))
    # Parse JSON
    logging.debug("Extracting labels UQD_multi")
    df['rater_labels'] = [UQD_extract_labels(x, use_extracted) for x in df[RATER_DECISION_COLUMN]]
    #df.to_csv('test-multi3.csv')
    # Count excluded json rows
    mask_rater = df['rater_labels'].isna() | df['rater_labels'].astype(str).str.lower().isin(['', 'na', 'null', 'nan'])
    info_multi_skipped_invalid_json = mask_rater.sum()
    # Remove invalid json rows
    df = df[~mask_rater].copy()


    # Filter out excluded labels
    df['rater_labels'] = df['rater_labels'].apply(UQD_filter_labels, args=(excluded_labels,))


    # Drop duplicates
    df_rater = UQD_drop_dupes(df, grouped_columns=["review_ds", "queue_name", "entity_id", "actor_id"], aggregated_columns=["rater_labels"])
    # Unpivot
    df_rater = UQD_unpivot_labels(df_rater,'rater')

    # Bulk rename to Universal format
    df_rater = UQD_bulk_rename_to_universal_format(df_rater)

    info_multi_rows_final = len(df_rater)
    info_multi = {
        #"methodology": "multi",
        "rows_initial": int(info_multi_rows_initial),
        "rows_final": int(info_multi_rows_final),
        "skipped_invalid_datetime": int(info_multi_skipped_invalid_datetime),
        "skipped_invalid_json": int(info_multi_skipped_invalid_json),
        "skipped_invalid_id": int(info_multi_skipped_invalid_id)
    }
    return df_rater, info_multi


def UQD_transform(df, stats, binary_labels, excluded_labels, use_extracted=False, quality_methods=None):
    # Strip initial spaces from column names
    df.columns = df.columns.str.strip()

    
    """
    # Check actor_id/job_id format
    df['actor_id'] = df['actor_id'].apply(UQD_int_number_check)
    df['job_id'] = df['job_id'].apply(UQD_int_number_check)

    # Skip row if actor_id or job_id is invalid
    df, skipped = UQD_filter_out_invalid_id_rows(df)
    if skipped > 0:
        logging.warning(f"Skipped {skipped} rows with invalid actor_id or job_id")
    """

    # Rename job_id column to entity_id
    df = df.rename(columns={"job_id": "entity_id"})

    # Split dataframe per methodology
    columns_to_keep = ["review_ds", "queue_name", "entity_id", "actor_id", "decision_data", "quality_actor_id", "quality_decision_data"]
    if use_extracted:
        columns_to_keep.extend(["extracted_label", "quality_extracted_label"])

    #quality_methodologies_file = df['quality_methodology'].str.lower().unique()
    #print(f"Quality methodologies found: {quality_methodologies_file}")

    # Transform Methodologies
    if quality_methods.get('audit', False):
        df_audit = df.loc[df['quality_methodology'].str.lower().str.contains('audit', na=False), columns_to_keep].copy()
        if not df_audit.empty:
            df_audit, info_audit = UQD_audit_transform(df_audit, binary_labels, excluded_labels, use_extracted)
            stats["audit"] = info_audit

    if quality_methods.get('multireview', False):
        df_multi = df.loc[df['quality_methodology'].str.lower().str.contains('multi', na=False), columns_to_keep].copy()
        if not df_multi.empty:
            df_multi, info_multi = UQD_multi_transform(df_multi, binary_labels, excluded_labels, use_extracted)
            stats["multi"] = info_multi

    if quality_methods.get('golden', False):
        df_golden = df.loc[df['quality_methodology'].str.lower().str.contains('golden', na=False), columns_to_keep].copy()
        if not df_golden.empty:
            df_golden, info_golden = UQD_golden_transform(df_golden, binary_labels, excluded_labels, use_extracted)
            stats["golden"] = info_golden


    # Combine dataframes
    dfs_to_concat = []
    if quality_methods.get('audit', False) and not df_audit.empty:
        dfs_to_concat.append(df_audit)
    if quality_methods.get('multireview', False) and not df_multi.empty:
        dfs_to_concat.append(df_multi)
    if quality_methods.get('golden', False) and not df_golden.empty:
        dfs_to_concat.append(df_golden)

    if dfs_to_concat: #At least one dataframe is not empty
        df_combined = pd.concat(dfs_to_concat, ignore_index=True, sort=False).fillna("")
    else:
        logging.error("No data available to concatenate. Please check input DataFrames")
        df_combined = pd.DataFrame()

    return df_combined


def transform(df, metadata):
    stats = {}
    stats["etl_module"] = "UQD"
    stats["rows_before_transformation"] = len(df)

    binary_labels = transformer_utils.get_binary_labels(metadata)
    excluded_labels = transformer_utils.get_excluded_labels(metadata)
    use_extracted = transformer_utils.get_use_extracted(metadata)
    quality_methods = transformer_utils.get_quality_methods(metadata)

    logging.info(f"Transforming UQD data with {len(binary_labels)} binary labels and {len(excluded_labels)} excluded labels")
    
    df = UQD_transform(df, stats, binary_labels, excluded_labels, use_extracted, quality_methods)
    stats["rows_after_transformation"] = len(df)
    logging.info(f"Transformed UQD data: {len(df)} rows")
    
    df = transformer_utils.enrich_dataframe_with_metadata(df, metadata)
    logging.info(f"Enriched UQD data with metadata")

    return df, stats