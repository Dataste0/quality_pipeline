###################
# a01Hs00001q1a2uIAA - 'Churchill' - Price and Availability
###################

import pandas as pd
from pipeline_lib.project_transformers.base_multi import base_transform as bmulti


# --- Logger
import logging
logger = logging.getLogger(__name__)



def adhoc_transform(df, stats, reporting_week):
    excluded_labels = ["main_image", "media", "videos"]

    df['job_date'] = pd.to_datetime(reporting_week, errors="coerce").strftime("%Y-%m-%d")
    df['workflow'] = "default_workflow"

    # Rename columns
    df.rename(columns={
        'reviewer_id': 'rater_id',
        'question': 'parent_label',
        'answer': 'rater_response'
    }, inplace=True)

    # Select relevant columns only
    info_cols = ["workflow", "job_date", "job_id", "rater_id"]
    df_selected = df[info_cols + ["parent_label", "rater_response"]]

    # Filter excluded labels
    if excluded_labels:
        df_filtered = df_selected[~df_selected['parent_label'].isin(excluded_labels)]
    else:
        df_filtered = df_selected
    df = df_filtered

    stats["excluded_list"] = excluded_labels
    stats["label_list"] = list(set(df["parent_label"].unique()))

    # Pivot labels
    df_pivot = (
        df
        .set_index(info_cols + ["parent_label"])  # indice “multi”
        ["rater_response"]                                                      # la serie da pivot
        .unstack("parent_label")                                                 # pivot
        .add_prefix("rater_")                                                    # aggiunge il prefisso “rater_” a tutti i nuovi nomi colonna
        .reset_index()                                                            # riporta l’indice nei campi normali
    )
  
    df = df_pivot

    print(f"\nDEBUG RETURNING DF COLS: {df.columns}")
    print(f"\nDEBUG RETURNING DF: {df.head()}")

    # [workflow, job_date, job_id, rater_id, ] [rater_label1, rater_label2, ...]

    return df






def transform(df, module_info):
    stats = {}
    stats["etl_module"] = "ADHOC-a01Hs00001q1a2uIAA"

    #print("METADATA: {mod_config}")

    # Module config
    reporting_week = module_info.get("reporting_week")

    logging.info(f"Transforming data with ADHOC module")
    
    stats["rows_before_transformation"] = len(df)
    df = adhoc_transform(df, stats, reporting_week)
    stats["rows_after_transformation"] = len(df)

    logging.info(f"Transformed data with ADHOC module: {len(df)} rows")
    
    #df = transformer_utils.enrich_dataframe_with_metadata(df, metadata)
    #logging.info(f"Enriched UQD data with metadata")

    
    module_info["base_config"] = {
        "labels": [
            {
                "label_name": label,
                "is_label_binary": False,
                "weight": 1
            }
            for label in stats.get("label_list", [])
        ]
    }

    base_config = module_info.get("base_config", {})
    base_df, base_info = bmulti(df, base_config)
    
    stats["base_info"] = base_info
    
    return base_df, stats