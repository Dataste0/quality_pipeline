###############
# BASE - RUBRIC
###############

# INPUT Taken
# [workflow, job_date, rater_id, auditor_id, job_id] [job_outcome, job_score] [rb_col1, rb_col2, rb_col3]

# OUTPUT
# [base, content_week] [workflow, job_date, rater_id, auditor_id, job_id] [final_job_tag, final_job_score] [rubric, factor, rubric_score]


import pandas as pd
import numpy as np

from pipeline_lib.project_transformers import transformer_utils

MODEL_BASE = "rubric"

DEFAULT_RUBRIC_NAME = "default_rubric"


def unpivot_labels(df, stats, group_cols, label_columns):
    missing_cols = [col for col in group_cols + label_columns if col not in df.columns]
    if missing_cols:
        stats["transform_error"] = f"Missing required columns in DataFrame: {missing_cols}"
        print(f"Missing required columns in DataFrame: {missing_cols}")
        return None

    df_unpivoted = df.melt(
        id_vars=group_cols,
        value_vars=label_columns,
        var_name="rubric",
        value_name="factor"
    )

    if not df_unpivoted.empty:
        df_unpivoted["rubric"] = df_unpivoted["rubric"].astype(str)
        df_unpivoted["rubric"] = df_unpivoted["rubric"].str.removeprefix("rb_")

    return df_unpivoted


def base_rubric_etl(df, stats, base_config):

    try:
        rubric_items = base_config.get("rubric", {})

        # Needed columns
        base_cols_list = ['workflow', 'job_date', 'rater_id', 'auditor_id', 'job_id', 'job_correct', 'job_score']
        rubric_list = [d.get("rubric_name") for d in rubric_items]
        rubric_cols_list = [d.get("rubric_column") for d in rubric_items]
       

        required_cols = base_cols_list + rubric_cols_list
        # Verifica che esistano nel df
        missing_cols = [col for col in required_cols if col not in df.columns]
        if missing_cols:
            stats["transform_error"] = f"Missing required info columns in DataFrame: {missing_cols}"
            print(stats["transform_error"])
            return None

        df = df[required_cols].copy()

        # Strip quotes from ID Columns
        for col in ["rater_id", "auditor_id", "job_id"]:
            df[col] = df[col].astype("string").str.strip("'")


        # Replace empty workflow with "default_workflow"
        df['workflow'] = df['workflow'].fillna('').astype(str)
        df.loc[df['workflow'].str.strip().str.lower() == '', 'workflow'] = 'default_workflow'

        # Replace empty auditor_id with Na
        df['auditor_id'] = df['auditor_id'].replace(r'^\s*$', np.nan, regex=True)

        
        # Convert rubric columns to int values
        for col in rubric_cols_list:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(int)
        
        # Add main_label column
        default_rubric_name = DEFAULT_RUBRIC_NAME
        df[default_rubric_name] = 1


        # Unpivot
        unpivot_columns = rubric_cols_list + [default_rubric_name]
        group_cols = base_cols_list
        unpivoted_df = unpivot_labels(df, stats, group_cols, unpivot_columns)
        df = unpivoted_df
        #df.to_csv('mytest.csv', index=False)

        #print(f"UNPIV cols\n {unpivoted_df.columns}")
        #print(f"UNPIV\n {unpivoted_df.head()}")
        #unpivoted_df.to_csv('test_unpivoted_halorubric.csv', index=False)

        
        # Assign penalties
        df["factor"] = pd.to_numeric(df["factor"], errors="coerce").fillna(1)
        
        penalty_map = {
            item["rubric_name"]: float(item.get("penalty_score", item.get("penalty_score", 0))) / 100.0
            for item in rubric_items
        }
        penalty_map[default_rubric_name] = float(0)
        # penalty_map = {
        #   rubric_name : rubric_penalty
        # }

        # Add rubric score
        df["rubric_penalty"] = df["rubric"].map(penalty_map)
        base_score = df["rubric_penalty"] * df["factor"]
        df["rubric_score"] = np.where(
            df["auditor_id"].isna(),
            np.nan,
            np.where(
                df["rubric"] == default_rubric_name,
                1,
                np.where(base_score > 0, base_score * -1, base_score)
            )
        )
        
        # Reorder
        final_cols = ['workflow', 'job_date', 'rater_id', 'auditor_id', 'job_id', 'job_correct', 'job_score', \
                      'rubric', 'rubric_penalty', 'factor', 'rubric_score']
        df = df[final_cols]

        # OUTPUT ['workflow', 'job_date', 'rater_id', 'auditor_id', 'job_id', 'job_correct', 'job_score', 'rubric', 'rubric_penalty', 'factor', 'rubric_score']
        return df

    except Exception as e:
        stats["transform_error"] = f"Unexpected error: {str(e)}"
        print(f"BASE RUBRIC ERROR: Unexpected error: {str(e)}")
        return None




def base_transform(df, base_config):
    stats = {}
    stats["model_base"] = f"BASE-{MODEL_BASE.upper()}"

    stats["rows_before_transformation"] = len(df)

    if not base_config:
        stats["transform_error"] = "base_config_missing"
        return pd.DataFrame(), stats
    
    df = base_rubric_etl(df, stats, base_config)
    df.insert(0, "base", MODEL_BASE)
    df.insert(1, "content_week", transformer_utils.compute_content_week(df["job_date"]))

    stats["rows_after_transformation"] = len(df)

    # Add project_id...
    #df = transformer_utils.enrich_dataframe_with_metadata(df, metadata)

    return df, stats

