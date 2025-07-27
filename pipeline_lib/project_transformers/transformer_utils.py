import pandas as pd
import importlib
import re
import logging
from pipeline_lib.project_transformers.universal_quality_config import universal_quality_columns
from pipeline_lib.project_transformers import mod_cvs, mod_ma, mod_uqd, mod_halo

# --- Setup logger
logger = logging.getLogger('pipeline.transform_modules')



#####################
# METADATA VALUES EXTRACTOR
#####################

def get_binary_labels(metadata):
    return metadata.get("binary_labels", {})

def get_excluded_labels(metadata):
    return metadata.get("excluded_labels", [])

def get_module(metadata):
    return metadata.get("module", None)

def get_quality_methods(metadata):
    return metadata.get("quality_methods", {})

def get_use_extracted(metadata):
    return metadata.get("use_extracted", False)

def get_project_id(metadata):
    return metadata.get("project_id", None)

def get_project_name(metadata):
    return metadata.get("project_name", None)

def get_label_dict(metadata):
    return metadata.get("label_dict", None)

def get_datafile_format(metadata):
    return metadata.get("datafile_format", {})



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


# Actor ID/Job ID check
def id_format_check(val):
    int_number_regex = re.compile(r'^\d+$')
    if isinstance(val, str) and int_number_regex.fullmatch(val) and 'e' not in val.lower():
        return val
    
    return None


#####################
# LABEL RANKING
#####################

def generate_label_dict(parent_labels: pd.Series) -> dict:
    """
    Generates a label_dict assigning a unique number to each parent_label.
    Labels are ordered by frequency (descending), then alphabetically for tie-breaks.
    Returns: {1: 'label_a', 2: 'label_b', ...}
    """
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
    """
    Inverts a label_dict from {number: label} to {label: number}
    """
    return {v: k for k, v in label_dict.items()}


def assign_question_number(df: pd.DataFrame, label_dict: dict | None) -> pd.DataFrame:
    """
    Assigns a question_number to each row based on label_dict and label_name column.
    If label_dict is None, a new one is generated from df.
    """
    if label_dict is None:
        label_dict = generate_label_dict(df['parent_label'])

    #print(f"Generated label_dict: {label_dict}")
    label_to_number = invert_label_dict(label_dict)

    df["question_number"] = df["parent_label"].map(label_to_number)

    return df


#####################
# DATAFRAME ENRICHER
#####################

def enrich_dataframe_with_metadata(df: pd.DataFrame, metadata: dict) -> pd.DataFrame:
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


#####################
# DISPATCHER SELECTOR
#####################

STANDARD_DISPATCHER = {
    "UQD": mod_uqd.transform,
    "CVS": mod_cvs.transform,
    "MA": mod_ma.transform,
    "HALO": mod_halo.transform
}

def get_transformer_from_metadata(metadata):
    module_key = get_module(metadata)
    project_id = get_project_id(metadata)

    if not module_key:
        raise ValueError("Missing 'module' key in metadata.")

    if module_key in STANDARD_DISPATCHER:
        return STANDARD_DISPATCHER[module_key]

    elif module_key == "ADHOC":
        if not project_id:
            raise ValueError("Missing 'project_id' in metadata for ADHOC module.")
        
        try:
#            adhoc_module = importlib.import_module(f"project_transformers.mod_{project_id}")
            adhoc_module = importlib.import_module(f"pipeline_lib.project_transformers.mod_{project_id}")
            return getattr(adhoc_module, "transform")
        except (ImportError, AttributeError) as e:
            raise ImportError(f"Failed to load ad-hoc module for project '{project_id}': {e}")

    else:
        raise ValueError(f"Unsupported module type: '{module_key}'")



#####################
# UNIVERSAL TRANSFORMER
#####################

def process_dataframe(df, metadata):
    transform_function = get_transformer_from_metadata(metadata)
    logging.info(f"Processing Dataframe using transformer: {transform_function.__module__}.{transform_function.__name__}")

    # Collecting info about the rawdata df
    info_dict = {
        "project_id": get_project_id(metadata),
        "project_name": get_project_name(metadata),
        "module": get_module(metadata),
        "row_count": len(df)
    }

    # Transform
    df_transformed, stats = transform_function(df, metadata)

    # Force all df data types to string
    df_transformed = df_transformed.astype("string")

    info_dict["transformed"] = stats

    return df_transformed, info_dict