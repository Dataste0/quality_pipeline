import pandas as pd
import re
import logging
#from pipeline_lib.project_transformers.schema import universal_quality_columns, halo_rubric_columns


#####################
# ID, DATE FORMAT
#####################

from dateutil import parser

def convert_tricky_date(date_value):
    if pd.isna(date_value) or str(date_value).strip() == "":
        return None

    # Try parsing with dateutil (very flexible parser)
    if isinstance(date_value, str):
        try:
            converted_date = parser.parse(date_value, fuzzy=True)
            return converted_date.strftime("%Y-%m-%d")
        except Exception:
            pass

    # Try parsing standard ISO formats
    try:
        converted_date = pd.to_datetime(date_value, errors='coerce')
        if pd.notna(converted_date):
            return converted_date.strftime("%Y-%m-%d")
    except Exception:
        pass

    # Try Excel serial date
    try:
        if isinstance(date_value, (int, float)) or (isinstance(date_value, str) and date_value.replace(".", "", 1).isdigit()):
            converted_date = pd.to_datetime(float(date_value), origin="1899-12-30", unit="D")
            return converted_date.strftime("%Y-%m-%d")
    except Exception as e:
        logging.warning(f"Failed to parse Excel date: {date_value}. Error: {e}")

    logging.warning(f"Unrecognized date format: {date_value}")
    return None


# Get content week
 
def compute_content_week(dates: pd.Series) -> pd.Series:
    dt = pd.to_datetime(dates, errors="coerce")
    weekday = dt.dt.weekday  # 0=Mon ... 6=Sun
    custom_weekday = (weekday + 2) % 7
    days_to_friday = (6 - custom_weekday) % 7
    return dt + pd.to_timedelta(days_to_friday, unit="d")

# Actor ID/Job ID check
def id_format_check(val):
    int_number_regex = re.compile(r'^\d+$')
    if isinstance(val, str) and int_number_regex.fullmatch(val) and 'e' not in val.lower():
        return val
    
    return None


#####################
# LABEL RANKING
#####################
"""
def generate_label_dict(parent_labels: pd.Series) -> dict:
    # Generates a label_dict assigning a unique number to each parent_label.
    # Labels are ordered by frequency (descending), then alphabetically for tie-breaks.
    # Returns: {1: 'label_a', 2: 'label_b', ...}

    # Filter out empty and NaN values
    filtered = parent_labels.dropna()
    filtered = filtered[filtered != ""]

    # Create frequency count
    counts = filtered.value_counts()

    # Convert to DataFrame for sorting
    df_counts = counts.reset_index()
    df_counts.columns = ["label", "count"]

    # Sort by count descending, then label ascending (alphabetical)
    df_counts = df_counts.sort_values(by=["count", "label"], ascending=[False, True])

    # Assign rank starting from 1
    label_dict = {str(i + 1): row["label"] for i, row in df_counts.iterrows()}

    return label_dict


def invert_label_dict(label_dict: dict) -> dict:
    # Inverts a label_dict from {number: label} to {label: number}
    return {v: k for k, v in label_dict.items()}


def assign_question_number(df: pd.DataFrame, label_dict: dict | None) -> pd.DataFrame:
    # Assigns a question_number to each row based on label_dict and label_name column.
    # If label_dict is None, a new one is generated from df.

    if label_dict is None:
        label_dict = generate_label_dict(df['parent_label'])

    #print(f"Generated label_dict: {label_dict}")
    label_to_number = invert_label_dict(label_dict)

    df["question_number"] = df["parent_label"].map(label_to_number)

    return df
"""

#####################
# DATAFRAME ENRICHER
#####################
"""
def enrich_dataframe_with_metadata(df: pd.DataFrame, metadata: dict) -> pd.DataFrame:
    module_used = get_module(metadata)

    if module_used == "HALO-RUBRIC":
        for col in halo_rubric_columns:
            #print(f"DF cols: {df.columns}")
            if col not in df.columns:
                #print(f"WARNING not in df cols: {col}")
                df[col] = ""
        
        # Reorder and keep only the Halo Rubric columns
        df = df[halo_rubric_columns].copy()

        # Add project_id
        project_id = get_project_id(metadata)
        df["project_id"] = project_id

        # Replace empty values for workflow
        df["workflow"] = df["workflow"].fillna("").astype(str).replace("", "default_workflow")

        # Trim workflow names
        df["workflow"] = df["workflow"].apply(
            lambda x: x if len(x) <= 51 else x[:25] + "_" + x[-25:]
        )


    else:
        # Ensure all universal quality columns exist
        for col in universal_quality_columns:
            if col not in df.columns:
                df[col] = ""

        # Reorder and keep only the universal columns
        df = df[universal_quality_columns].copy()

        # Assign question_number
        label_dict = get_label_dict(metadata)
        df = assign_question_number(df, label_dict)

        # Add project_id
        project_id = get_project_id(metadata)
        df["project_id"] = project_id

        # Replace empty values for workflow
        df["workflow"] = df["workflow"].fillna("").astype(str).replace("", "default_workflow")
        
        # Trim workflow names
        df["workflow"] = df["workflow"].apply(
            lambda x: x if len(x) <= 51 else x[:25] + "_" + x[-25:]
        )
    
    return df
    #return df, label_dict

"""