import importlib
from turtle import pd
from pipeline_lib.project_transformers import mod_cvs, mod_uqd, mod_halo, mod_generic
from pipeline_lib.project_transformers.transformer_utils import compute_content_week, column_replacer, string_replacer, regex_replacer

# --- Logger
import logging
logger = logging.getLogger(__name__)


STANDARD_DISPATCHER = {
    "UQD": mod_uqd.transform,
    "CVS": mod_cvs.transform,
    "HALO": mod_halo.transform,
    "GENERIC": mod_generic.transform
}

def get_transformer_from_metadata(project_id, module_key):
    if not module_key:
        raise ValueError("Missing 'module' key in metadata.")

    if module_key in STANDARD_DISPATCHER:
        return STANDARD_DISPATCHER[module_key]

    elif module_key == "ADHOC":
        if not project_id:
            raise ValueError("Missing 'project_id' in metadata for ADHOC module.")
        
        try:
            adhoc_module = importlib.import_module(f"pipeline_lib.project_transformers.mod_{project_id}")
            return getattr(adhoc_module, "transform")
        except (ImportError, AttributeError) as e:
            raise ImportError(f"Failed to load ad-hoc module for project '{project_id}': {e}")

    else:
        raise ValueError(f"Unsupported module type: '{module_key}'")



#####################
# UNIVERSAL TRANSFORMER
#####################

def process_dataframe(df, project_metadata):
    project_id = project_metadata.get("project_id")
    module = project_metadata.get("project_config", {}).get("module")
    transform_function = get_transformer_from_metadata(project_id, module)


    # Collecting info about the rawdata df
    processed_dict = {
        "module_used": module,
        "row_count": len(df)
    }

    # Pre-processing
    module_config = project_metadata.get("project_config", {}).get("module_config", {})
    [column_replacer(df, item) for item in module_config.get("replace_columns", [])]
    [string_replacer(df, item) for item in module_config.get("replace_strings", [])]
    [regex_replacer(df, item) for item in module_config.get("replace_regex", [])]
    

    # Transform
    df_transformed, etl_stats = transform_function(df, project_metadata)

    # Add content week column
    if not df_transformed.empty and "job_date" in df_transformed.columns:
        content_week_serie = compute_content_week(df_transformed["job_date"])
        df_transformed.insert(0, "content_week", content_week_serie)

    processed_dict["etl"] = etl_stats

    return df_transformed, processed_dict