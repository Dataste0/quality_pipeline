###################
# Project 'Query-Category Classifier Accuracy'
# -- Fix bad escaping, then parse with UQD - Using Extracted
###################

from pipeline_lib.project_transformers.mod_uqd import transform as uqd_transform
import logging

# --- Setup logger
logger = logging.getLogger('pipeline.transform_modules')



def adhoc_transform(df):
    df['extracted_label'] = df['extracted_label'].str.replace("'s", "’s", regex=False)
    df['quality_extracted_label'] = df['quality_extracted_label'].str.replace("'s", "’s", regex=False)
    return df


def transform(df, metadata):
    stats = {}
    stats["etl_module"] = "ADHOC-a01Hs00001ocUaAIAU"
    stats["rows_before_transformation"] = len(df)

    df = adhoc_transform(df)
    df, df_stats = uqd_transform(df, metadata)
    
    stats["transformation"] = df_stats
    stats["rows_after_transformation"] = len(df)

    return df, stats