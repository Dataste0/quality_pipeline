###############
# BASE - MULTI
###############

# INPUT Taken
# [workflow, job_date, rater_id, job_id] \\ [r_label1, r_label2, r_label3] ...

# OUTPUT
# [base, content_week] [workflow, job_date, rater_id, job_id] \\
# [parent_label, rater_response, is_label_binary, weight]


import pandas as pd
import numpy as np
import logging
from pipeline_lib.project_transformers import transformer_utils

MODEL_BASE = "multi"



def base_multi_etl(df, stats, base_config):
    """
    BASE_CONFIG EXAMPLE

    base_config = {
        
        "labels": [
            {
                "label_name": "able_to_eval",
                "rater_label_column": "r_able_to_eval", 
                "is_label_binary": true,
                "label_binary_pos_value": "EB_yes",
                "weight": 1
            },
            {
                "label_name": "withhold",
                "rater_label_column": "r_withhold", 
                "is_label_binary": false,
                "weight": null
            }
        ]
    }
    """







    try:
        label_dicts = base_config.get("labels", [])

        # Map label columns
        label_column_map = {
            f"rater_{v['label_name']}": f"{v['label_name']}|rater"
            for v in label_dicts
        }
        df = df.rename(columns=label_column_map)

       
        # Needed columns
        info_columns = ["workflow", "job_date", "rater_id", "job_id"]
        label_cols_renamed = list(label_column_map.values())

        required_df_cols = info_columns + label_cols_renamed
        df = df.loc[:, [col for col in required_df_cols if col in df.columns]]


        # ["workflow", "job_date", "rater_id", "job_id"] [is_rateable|rater] [withhold|rater] ...
        
        # Unpivot
        df_unpivoted = df.melt(
            id_vars=info_columns,
            value_vars=label_cols_renamed,
            var_name="label_role",
            value_name="rater_response"
        )
        

        df_unpivoted[["parent_label", "role"]] = df_unpivoted["label_role"].str.rsplit("|", n=1, expand=True)

        # ["workflow", "job_date", "rater_id", "job_id"] [is_rateable|rater] [withhold|rater]
        df = df_unpivoted

        binary_map = {
            d["label_name"]: bool(d.get("is_label_binary", False))
            for d in label_dicts
            if "label_name" in d
        }
        df["is_label_binary"] = df["parent_label"].map(binary_map).fillna(False).astype("boolean")

        pos_value_map = {
            d["label_name"]: d.get("label_binary_pos_value", "True")
            for d in label_dicts
            if "label_name" in d
        }        
        expected_pos = df["parent_label"].map(pos_value_map)
        df["is_positive"] = (df["rater_response"] == expected_pos).where(df["is_label_binary"]).astype("boolean")

        weight_map = {
            d["label_name"]: int(d["weight"]) if d.get("weight") is not None else 1
            for d in label_dicts
            if "label_name" in d
        }
        df["weight"] = df["parent_label"].map(weight_map).fillna(1).astype(int)

        final_columns = info_columns + ["parent_label", "rater_response", "is_label_binary", "is_positive", "weight"]
        df = df[final_columns]
        
        return df

    except Exception as e:
        stats["transform_error"] = f"Unexpected error: {str(e)}"
        print(f"BASE MULTI ERROR: Unexpected error: {str(e)}")
        return None




def base_transform(df, base_config):
    stats = {}
    stats["model_base"] = f"BASE-{MODEL_BASE.upper()}"

    stats["rows_before_transformation"] = len(df)

    if not base_config:
        stats["transform_error"] = "base_config_missing"
        return pd.DataFrame(), stats
    
    df = base_multi_etl(df, stats, base_config)
    
    df.insert(0, "base", MODEL_BASE)
    df.insert(1, "content_week", transformer_utils.compute_content_week(df["job_date"]))


    stats["rows_after_transformation"] = len(df)

    # Add project_id...
    #df = transformer_utils.enrich_dataframe_with_metadata(df, metadata)

    df.to_csv("base_multi_debug_output.csv", index=False)

    return df, stats