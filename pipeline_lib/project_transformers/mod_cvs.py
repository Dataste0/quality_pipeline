import pandas as pd
import json
import logging
from pipeline_lib.project_transformers import transformer_utils

# --- Setup logger
logger = logging.getLogger('pipeline.transform_modules')



# Extract labels to a list from JSON string
def CVS_extract_labels(json_str):
    try:
        #logging.debug(f"Extract label from: {type(json_str)} {repr(json_str)}")
        if not isinstance(json_str, str) or pd.isna(json_str):
            logging.warning("JSON is None or not a string")
            return None

        json_str = json_str.replace("'", '"')

        # Parse first-level JSON
        first_level_json = json.loads(json_str)
        #logging.debug(f"PARSED {type(first_level_json)} {repr(first_level_json)}")

        first_level_key = next(iter(first_level_json), None)
        #logging.debug(f"FIRST LEVEL KEY: {type(first_level_key)} {first_level_key}")

        # Handle different JSON types
        if first_level_key ==  'decision_string':
            #logging.debug(f"FIRST LEVEL KEY 'DECISION STRING': {first_level_json}")
            labels = first_level_json.get("labels", None)
            #logging.debug(f"EXTRACTED LABELS ({len(labels)}): {type(labels)} {labels}\n")
            return labels

        elif first_level_key.isdigit():
            #logging.debug(f"FIRST LEVEL KEY 'DIGIT': {first_level_json}")
            auditor_id = first_level_key
            #logging.debug(f"AUDITOR ID: {auditor_id}")
            # Extract nested JSON (if exists)
            nested_json_str = first_level_json.get(auditor_id, None)

            if nested_json_str and isinstance(nested_json_str, str):
                # Parse the nested JSON string
                nested_data = json.loads(nested_json_str)
                #logging.debug(f"Parsed nested Auditor JSON: {nested_data}")
                # Extract auditor labels
                auditor_labels = nested_data.get("labels", None)
                #logging.debug(f"Extracted auditor labels: {auditor_labels}")
                return auditor_id, auditor_labels
            else:
                logging.warning('Error: no nested JSON in Auditor data')
                return None

        else:
            logging.warning("Unknown JSON format")
            return None

    except json.JSONDecodeError as e:
        logging.warning(f"JSON Decode Error: {e} - {json_str}")
        return None
    except Exception as e:
        logging.warning(f"Unexpected Error: {e}")
        return None


# Filter out excluded labels
def CVS_filter_labels(label_list, excluded_list):
    if not isinstance(label_list, list):
        logging.error(f"Filter Labels - Expected a list of labels, got: {label_list}")
        return None

    return [item for item in label_list if item.split("::")[0] not in excluded_list]



# Compute confusion type for rater_labels
def CVS_compute_confusion_type(rater_labels, auditor_labels, binary_labels):
    if not isinstance(rater_labels, list) or not isinstance(auditor_labels, list):
        logging.error(f"Compute Confusion - Format error: expected list. Rater labels: {rater_labels}. Auditor labels: {auditor_labels}")
        return None

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
            logging.error(f"Compute Confusion - Incorrect format")
            return None
    return updated_labels


# Drop duplicates
def CVS_drop_dupes(df, grouped_columns, aggregated_columns):
    # Mask to exclude rows with NaN or empty strings in - at least - 1 of the selected_columns
    mask = df[aggregated_columns].notna().all(axis=1) & (df[aggregated_columns] != "").all(axis=1)
    df_clean = df[mask]
    # Removes dupes
    df_deduped = df_clean.drop_duplicates(subset=grouped_columns, keep='first')
    return df_deduped[grouped_columns + aggregated_columns]


# Explode labels
def CVS_unpivot_labels(df, role):
    labels_col = f"{role}_labels"

    # Explode the labels list into separate rows
    df_exploded = df.explode(labels_col).dropna(subset=[labels_col])

    # Extract attribute (label key) and value
    df_exploded["label_name"], df_exploded["label_response"] = zip(*df_exploded[labels_col].apply(lambda x: x.split("::", 1) if "::" in x else (x, None)))

    # Split confusion type from value
    df_exploded["label_response"], df_exploded["confusion_type"] = zip(*df_exploded["label_response"].apply(lambda x: x.split("|", 1) if "|" in x else (x, None)))

    # Drop the original labels column
    df_exploded = df_exploded.drop(columns=[labels_col])

    df_exploded["confusion_type"] = df_exploded["confusion_type"].fillna("")

    df_exploded["is_audit"] = 1 if role == "auditor" else 0
    df_exploded["source_of_truth"] = 1 if role == 'golden' else 0

    return df_exploded

# Bulk rename columns
def CVS_bulk_rename_to_universal_format(df):
    rename_dict = {
        "rater_id"            : "actor_id",
        "auditor_id"          : "actor_id",
        "quality_actor_id"    : "actor_id",
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



def CVS_transform(df, stats, binary_labels, excluded_labels):
    # Strip initial spaces from column names
    df.columns = df.columns.str.strip()

    # Fix date format and remove rows with incorrect dates
    df['sample_ds'] = df['sample_ds'].apply(transformer_utils.convert_tricky_date)
    stats["skipped_invalid_datetime"] = int(df['sample_ds'].isnull().sum())
    df = df[df['sample_ds'].notnull()].copy()

    # Filter out invalid sample date / empty rows
    #df = df[df["sample_ds"].notna() & (df["sample_ds"] != "")]
    # Convert dates
    #df["sample_ds"] = df["sample_ds"].apply(CVS_convert_date)

    # Filter out combined_routing which is not used
    df = df[df['routing_name'] != 'combined_routing']

    # Keep only relevant columns
    columns_to_keep = ["sample_ds", "routing_name", "entity_id", "rater_id", "rater_decision_data", "auditor_decision_data"]
    df = df[columns_to_keep].copy()

    # Fix JSON separator
    RATER_DECISION_COLUMN = "rater_decision_data"
    AUDITOR_DECISION_COLUMN = "auditor_decision_data"
    df[RATER_DECISION_COLUMN] = df[RATER_DECISION_COLUMN].fillna("").str.replace(";", ",")
    df[AUDITOR_DECISION_COLUMN] = df[AUDITOR_DECISION_COLUMN].fillna("").str.replace(";", ",")

    # Parse Rater JSON
    df['rater_labels'] = [CVS_extract_labels(x) for x in df[RATER_DECISION_COLUMN]]

    # Parse Auditor JSON (extract auditor_id and auditor_labels if CVS)
    parsed_auditor = [CVS_extract_labels(x) for x in df[AUDITOR_DECISION_COLUMN]]
    df["auditor_id"] = [x[0] for x in parsed_auditor]
    df["auditor_labels"] = [x[1] for x in parsed_auditor]

    # Filter out Excluded labels from rater_labels and auditor_labels
    df['rater_labels'] = df['rater_labels'].apply(CVS_filter_labels, args=(excluded_labels,))
    df['auditor_labels'] = df['auditor_labels'].apply(CVS_filter_labels, args=(excluded_labels,))

    # Compute confusion_type and concatenate it to rater binary labels in rater_labels column
    df["rater_labels"] = [
        CVS_compute_confusion_type(rater, auditor, binary_labels)
        for rater, auditor in zip(df["rater_labels"], df["auditor_labels"])
    ]

    #######################
    # Count excluded json rows
    mask_rater = df['rater_labels'].isna() | df['rater_labels'].astype(str).str.lower().isin(['', 'na', 'null', 'nan'])
    mask_auditor = df['auditor_labels'].isna() | df['auditor_labels'].astype(str).str.lower().isin(['', 'na', 'null', 'nan'])
    stats["skipped_invalid_json"] = int((mask_rater | mask_auditor).sum())
    # Remove invalid json rows
    df = df[~(mask_rater | mask_auditor)].copy()

    # ID Format check
    df['rater_id'] = df['rater_id'].apply(transformer_utils.id_format_check)
    df['auditor_id'] = df['auditor_id'].apply(transformer_utils.id_format_check)
    df['entity_id'] = df['entity_id'].apply(transformer_utils.id_format_check)
    # Count invalid IDs
    mask_invalid_id = df[['rater_id', 'auditor_id', 'entity_id']].isnull().any(axis=1)
    stats["skipped_invalid_id"] = int(mask_invalid_id.sum())
    # Remove from df
    df = df[~mask_invalid_id].copy()
    #########################

    # Drop duplicates
    df_rater = CVS_drop_dupes(df, grouped_columns=["sample_ds", "routing_name", "entity_id", "rater_id"], aggregated_columns=["rater_labels"])
    df_auditor = CVS_drop_dupes(df, grouped_columns=["routing_name", "entity_id"], aggregated_columns=["auditor_id", "auditor_labels", "sample_ds"])

    # Unpivot labels
    df_rater = CVS_unpivot_labels(df_rater,'rater')
    df_auditor = CVS_unpivot_labels(df_auditor,'auditor')

    # Bulk rename to Universal format
    df_rater = CVS_bulk_rename_to_universal_format(df_rater)
    df_auditor = CVS_bulk_rename_to_universal_format(df_auditor)

    # Combine dataframes
    df_combined = pd.concat([df_rater, df_auditor], ignore_index=True, sort=False).fillna("")

    return df_combined


def transform(df, metadata):
    """
    Entry point for CVS projects. Extracts parameters from metadata and calls CVS_transform.
    """
    stats = {}
    stats["etl_module"] = "CVS"
    stats["rows_before_transformation"] = len(df)

    binary_labels = transformer_utils.get_binary_labels(metadata)
    excluded_labels = transformer_utils.get_excluded_labels(metadata)

    logging.info(f"Transforming CVS data with {len(binary_labels)} binary labels and {len(excluded_labels)} excluded labels")
    
    df = CVS_transform(df, stats, binary_labels, excluded_labels)
    stats["rows_after_transformation"] = len(df)
    logging.info(f"Transformed CVS data: {len(df)} rows")
    
    df = transformer_utils.enrich_dataframe_with_metadata(df, metadata)
    logging.info(f"Enriched CVS data with metadata")
    
    return df, stats
