###################
# Project 'Query-Category Classifier Accuracy'
# -- Fix bad escaping, then parse with UQD - Using Extracted
###################

from pipeline_lib.project_transformers.mod_uqd import transform as uqd_transform

# --- Logger
import logging
logger = logging.getLogger(__name__)



def adhoc_transform(df):
    df['extracted_label'] = df['extracted_label'].str.replace("'s", "’s", regex=False)
    df['quality_extracted_label'] = df['quality_extracted_label'].str.replace("'s", "’s", regex=False)
    return df


def transform(df, metadata):

    stats = {}
    stats["etl_module"] = "ADHOC-a01Hs00001ocUaAIAU"
    stats["rows_before_transformation"] = len(df)

    df = adhoc_transform(df)

    # Force use_extracted:False
    mod_info = {
        "module": "UQD",
        "module_config": {
            "use_extracted": True,
            "quality_methodology": "audit",
            "excluded_labels": [],
            "binary_labels": []
        }
    }

    df, df_stats = uqd_transform(df, mod_info)
    stats["etl"] = df_stats

    stats["rows_after_transformation"] = len(df)
    return df, stats