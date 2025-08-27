import pandas as pd
import json
from pipeline_lib.project_transformers import transformer_utils as tu

# --- Logger
import logging
logger = logging.getLogger(__name__)


# CVS Default Columns
CVS_RATER_ID_COL_NAME = "rater_id"
CVS_AUDITOR_ID_COL_NAME = "quality_actor_id"
CVS_JOB_ID_COL_NAME = "entity_id"
CVS_SUBMISSION_DATE_COL_NAME = "sample_ds"
CVS_WORKFLOW_COL_NAME = "routing_name"
CVS_RATER_DECISION_DATA_COL_NAME = "rater_decision_data"
CVS_AUDITOR_DECISION_DATA_COL_NAME = "auditor_decision_data"



# Extract labels to a list from JSON string
def cvs_extract_labels(json_str):
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




def cvs_transform(df, stats, mod_config):
    """
    "module_config": {
              "excluded_labels": [],
              "binary_labels": [
                  {
                    "label_name": "withhold",
                    "binary_positive_value": "yes"
                  },
                  {
                    "label_name": "exaggeration",
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

    quality_methodology = "audit"
    excluded_list = mod_config.get("excluded_labels", [])
    binary_labels = mod_config.get("binary_labels", [])
    label_weights = mod_config.get("label_weights", {})

    stats["quality_methodology"] = quality_methodology
    stats["excluded_list"] = excluded_list
    stats["binary_labels"] = binary_labels
    stats["label_weights"] = label_weights

    # Strip initial spaces from column names
    df.columns = df.columns.str.strip()

    # Map columns
    column_map = {
        CVS_RATER_ID_COL_NAME                   : "rater_id",
        CVS_AUDITOR_ID_COL_NAME                 : "auditor_id",
        CVS_JOB_ID_COL_NAME                     : "job_id",
        CVS_SUBMISSION_DATE_COL_NAME            : "job_date",
        CVS_WORKFLOW_COL_NAME                   : "workflow",
        CVS_RATER_DECISION_DATA_COL_NAME        : "rater_decision_data",
        CVS_AUDITOR_DECISION_DATA_COL_NAME      : "auditor_decision_data"
    }
    df.rename(columns=column_map, inplace=True)

    # Fix date format and remove rows with incorrect dates
    df["job_date"] = df["job_date"].apply(tu.convert_tricky_date)
    stats["skipped_invalid_datetime"] = int(df["job_date"].isnull().sum())
    df = df[df["job_date"].notnull()].copy()

    # Filter out combined_routing which is not used
    df = df[df["workflow"] != 'combined_routing']

    # Keep only relevant columns
    columns_to_keep = ["job_date", "workflow", "job_id", "rater_id", "rater_decision_data", "auditor_decision_data"]
    df = df[columns_to_keep].copy()

    # Fix JSON separator
    df["rater_decision_data"] = df["rater_decision_data"].fillna("").str.replace(";", ",")
    df["auditor_decision_data"] = df["auditor_decision_data"].fillna("").str.replace(";", ",")

    # Parse Rater JSON
    df['rater_labels'] = [cvs_extract_labels(x) for x in df["rater_decision_data"]]

    # Parse Auditor JSON (extract auditor_id and auditor_labels if CVS)
    parsed_auditor = [cvs_extract_labels(x) for x in df["auditor_decision_data"]]
    df["auditor_id"] = [x[0] for x in parsed_auditor]
    df["auditor_labels"] = [x[1] for x in parsed_auditor]

    df = df.drop(columns=["rater_decision_data", "auditor_decision_data"], errors="ignore")

    # Drop duplicates
    df.drop_duplicates(subset=["rater_id", "job_id"], keep="last", inplace=True)

    #######################
    # Count excluded json rows
    mask_rater = df['rater_labels'].isna() | df['rater_labels'].astype(str).str.lower().isin(['', 'na', 'null', 'nan'])
    mask_auditor = df['auditor_labels'].isna() | df['auditor_labels'].astype(str).str.lower().isin(['', 'na', 'null', 'nan'])
    stats["skipped_invalid_json"] = int((mask_rater | mask_auditor).sum())
    # Remove invalid json rows
    df = df[~(mask_rater | mask_auditor)].copy()

    # ID Format check
    df["rater_id"] = df["rater_id"].apply(tu.id_format_check)
    df["job_id"] = df["job_id"].apply(tu.id_format_check)
    # Count invalid IDs
    mask_invalid_id = df[["rater_id", "job_id"]].isnull().any(axis=1)
    stats["skipped_invalid_id"] = int(mask_invalid_id.sum())
    # Remove from df
    df = df[~mask_invalid_id].copy()
    #########################

    # Expand key values
    rater_labels_pivoted = tu.expand_label_columns(df, "rater_labels", "r", excluded_list)
    auditor_labels_pivoted = tu.expand_label_columns(df, "auditor_labels", "a", excluded_list)

    # Concatenate
    df = pd.concat([df, rater_labels_pivoted, auditor_labels_pivoted], axis=1)

    # Extract labels found and create a list (all_labels)
    def extract_labels(expanded_df, prefix):
        keys = []
        for col in expanded_df.columns:
            if col.startswith(f"{prefix}_"):
                keys.append(col[len(prefix)+1 :])  # rimuove "r_" o "a_"
        return set(keys)

    rater_keys = extract_labels(rater_labels_pivoted, "r")
    auditor_keys = extract_labels(auditor_labels_pivoted, "a")
    all_labels = sorted(rater_keys.union(auditor_keys))
    stats["label_list"] = all_labels
    
    # Rebuild df
    base_cols = ["workflow", "job_date", "rater_id", "auditor_id", "job_id"]
    base_df = df[base_cols].copy()

    # concateno: base + pivotate
    to_concat = [base_df, rater_labels_pivoted, auditor_labels_pivoted]
    result = pd.concat(to_concat, axis=1)

    df = result.drop(columns=["rater_labels", "auditor_labels"], errors="ignore")


    # Now let's unpivot parent labels
    df_long = tu.to_long(df, base_cols, all_labels)

    #[workflow, job_date, rater_id, auditor_id, job_id] [parent_label] [rater_response, auditor_response]

    df = tu.add_binary_flags(df_long, binary_labels)

    #[workflow, job_date, rater_id, auditor_id, job_id] [parent_label] [rater_response, auditor_response] [is_label_binary, confusion_type]

    df = tu.add_responses_match(df, col_name="is_correct", case_sensitive=False, strip=True)

    #[workflow, job_date, rater_id, auditor_id, job_id] [parent_label] [rater_response, auditor_response] [is_label_binary, confusion_type] [is_correct]

    df["weight"] = df["parent_label"].map(label_weights).fillna(1).astype(float)

    #[workflow, job_date, rater_id, auditor_id, job_id] [parent_label] [rater_response, auditor_response] [is_label_binary, confusion_type] [is_correct] [weight]



    # Compile stats
    stats["rows_final"] = len(df)


    return df


def transform(df, project_metadata):
    stats = {}
    stats["etl_module"] = "CVS"
    
    # Module config
    project_config = project_metadata.get("project_config", {})
    module_config = project_config.get("module_config", {})

    stats["rows_before_transformation"] = len(df)
    df = cvs_transform(df, stats, module_config)
    stats["rows_after_transformation"] = len(df) if df is not None else 0

    
    return df, stats
