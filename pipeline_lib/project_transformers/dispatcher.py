import importlib
from pipeline_lib.project_transformers import mod_cvs, mod_uqd, mod_halo, mod_generic

# --- Logger
import logging
logger = logging.getLogger(__name__)


STANDARD_DISPATCHER = {
    "UQD": mod_uqd.transform,
    "CVS": mod_cvs.transform,
    "HALO": mod_halo.transform,
    "GENERIC": mod_generic.transform
}

def get_transformer_from_metadata(metadata):
    module_key = metadata.get("module")
    project_id = metadata.get("project_id")

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

def process_dataframe(df, module_info):
    transform_function = get_transformer_from_metadata(module_info)
    #print(f"Processing Dataframe using transformer: {transform_function.__module__}.{transform_function.__name__}")

    #print(f"\nModule info: {module_info}\n")

    #print(f"\nProcess dataframe metadata received: {type(metadata)}\n{metadata}\n")

    # Collecting info about the rawdata df
    info_dict = {
        "module": module_info.get("module"),
        "row_count": len(df)
    }

    # Transform
    df_transformed, etl_stats = transform_function(df, module_info)

    # Force all df data types to string
    #df_transformed = df_transformed.astype("string")

    info_dict["etl"] = etl_stats

    return df_transformed, info_dict