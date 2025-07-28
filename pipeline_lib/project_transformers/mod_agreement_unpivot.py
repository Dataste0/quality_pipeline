import pandas as pd
import logging
from pipeline_lib.project_transformers import transformer_utils


# --- Setup logger
logger = logging.getLogger('pipeline.transform_modules')


def unpivot_labels(df, stats, label_columns):
    id_vars = ["job_id", "actor_id", "role", "submission_date", "workflow"]

    missing_cols = [col for col in id_vars + label_columns if col not in df.columns]
    if missing_cols:
        stats["transform_error"] = f"Missing required columns in DataFrame: {missing_cols}"
        print(f"Missing required columns in DataFrame: {missing_cols}")
        return None

    df_unpivoted = df.melt(
        id_vars=id_vars,
        value_vars=label_columns,
        var_name="parent_label",
        value_name="response_data"
    )

    return df_unpivoted


def melt_rater_auditor(df, stats, column_dict, excluded_labels):

    """
    column_dict example: 
    {
        metadata_columns: {
            "rater_id_column_name": "example",
            "auditor_id_column_name": "example",
            "job_id_column_name": "example",
            "submission_date_column_name": "example",
            "workflow_column_name": "workflow"  # optional
        },
        
        label_columns: {
            "label_0": "labelcolumn",
            "label_1": "labelcolumn",
            "label_2": "labelcolumn
        }
    }
    """

    try:
        
        metadata_keys = column_dict.get("metadata_columns", {})
        label_columns_map = column_dict.get("label_columns", {})

        rename_label_map = {
            v["label_name"]: v["label_replace"]
            for v in label_columns_map.values()
        }
        df = df.rename(columns=rename_label_map)

        excluded = [col for col in rename_label_map.values() if col in excluded_labels]
        df = df.drop(columns=excluded, errors='ignore')
        label_columns = [col for col in rename_label_map.values() if col not in excluded_labels and col in df.columns]

        required_meta_keys = ['rater_id_column_name', 'auditor_id_column_name', 'job_id_column_name', 'submission_date_column_name']
        missing_keys = [k for k in required_meta_keys if k not in metadata_keys]
        if missing_keys:
            stats["transform_error"] = f"Missing required keys in column_dict['metadata_columns']: {missing_keys}"
            print(f"Missing required keys in column_dict['metadata_columns']: {missing_keys}")
            return None

        rater_id_col = metadata_keys['rater_id_column_name']
        auditor_id_col = metadata_keys['auditor_id_column_name']
        job_id_col = metadata_keys['job_id_column_name']
        submission_date_col = metadata_keys['submission_date_column_name']
        workflow_col = metadata_keys.get('workflow_column_name', 'workflow')  # default se non c'è

        column_names_map = {
            rater_id_col: "rater_id",
            auditor_id_col: "auditor_id",
            job_id_col: "job_id",
            submission_date_col: "submission_date",
            workflow_col: "workflow"
        }

        df = df.rename(columns=column_names_map)

        if "workflow" not in df.columns:
            df["workflow"] = "default_workflow"

        required_df_cols = ["rater_id", "auditor_id", "job_id", "submission_date", "workflow"] + label_columns
        missing_df_cols = [col for col in required_df_cols if col not in df.columns]
        if missing_df_cols:
            stats["transform_error"] = f"Missing required columns in DataFrame: {missing_df_cols}"
            return None

        df = df.copy()

        df["submission_date"] = df["submission_date"].apply(transformer_utils.convert_tricky_date)

        # Rater: all labels to True
        rater_df = df[["rater_id", "job_id", "submission_date", "workflow"]].copy()
        for col in label_columns:
            rater_df[col] = True
        rater_df['role'] = 'rater'
        rater_df = rater_df.rename(columns={"rater_id": "actor_id"})

        # Auditor: True for agreement, False for disagreement
        auditor_df = df[["auditor_id", "job_id", "submission_date", "workflow"] + label_columns].copy()
        auditor_df = auditor_df.drop_duplicates(subset=["auditor_id", "job_id"])
        auditor_df['role'] = 'auditor'
        auditor_df = auditor_df.rename(columns={"auditor_id": "actor_id"})

        # Unpivot
        rater_unpivoted_df = unpivot_labels(rater_df, stats, label_columns)
        auditor_unpivoted_df = unpivot_labels(auditor_df, stats, label_columns)
        auditor_unpivoted_df = auditor_unpivoted_df.drop_duplicates(subset=["job_id", "actor_id", "parent_label"])

        # Reorder
        final_columns = ["job_id", "actor_id", "role", "submission_date", "workflow", "parent_label", "response_data"]
        rater_unpivoted_df = rater_unpivoted_df[final_columns].copy()
        auditor_unpivoted_df = auditor_unpivoted_df[final_columns].copy()
       
        df = pd.concat([rater_unpivoted_df, auditor_unpivoted_df], ignore_index=True)
        df["role"] = df["role"].map({"auditor": 1, "rater": 0})

        return df

    except Exception as e:
        stats["transform_error"] = f"Unexpected error: {str(e)}"
        print(f"ERROR: Unexpected error: {str(e)}")
        return None




def transform(df, metadata):
    """
    COLUMN_DICT EXAMPLE

    column_dict = {
        
        "metadata_columns": {
            "rater_id_column_name": "Annotator ID",
            "auditor_id_column_name": "Auditor ID",
            "job_id_column_name": "Annotation Job ID",
            "submission_date_column_name": "Annotation Date And Time"
        },
        
        "label_columns": {
            "label_0": {
                "label_name" : "KPI: Is URL classified correctly?", 
                "label_replace" : "URL classified correctly"
            },
            "label_1": {
                "label_name" : "KPI: Is Title annotated correctly?", 
                "label_replace" : "Title annotated correctly"
            },
            "label_2": {
                "label_name" : "KPI: Is main-description annotated correctly?", 
                "label_replace" : "main-description annotated correctly"
            },
            "label_3": {
                "label_name" : "KPI: Is additional-description annotated correctly?", 
                "label_replace" : "additional-description annotated correctly"
            }
        }
    }
    """

    stats = {}
    stats["etl_module"] = "AU"
    stats["rows_before_transformation"] = len(df)

    excluded_labels = transformer_utils.get_excluded_labels(metadata)
    column_dict = metadata.get("mod_config", None)

    if not column_dict:
        stats["transform_error"] = "module_config_missing"
        return pd.DataFrame()

    df = melt_rater_auditor(df, stats, column_dict, excluded_labels)

    stats["rows_after_transformation"] = len(df)

    df = transformer_utils.enrich_dataframe_with_metadata(df, metadata)

    return df