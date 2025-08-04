###################
# Project 'ML Text Generation MAdLlama'
# -- Old data format: handle with adhoc transformer
# -- New data format: handle with UQD transformer
###################

from pipeline_lib.project_transformers.mod_uqd import transform as uqd_transform
import re
import logging

# --- Setup logger
logger = logging.getLogger('pipeline.transform_modules')



def fix_apostrophes(text):
    if isinstance(text, str):    
        # Replace apostrophes between letters
        text = re.sub(r"(?<=[a-zA-Z])'(?=[a-zA-Z])", "’", text)
        # Replace apostrophes after 's' that are not followed by a letter but followed by a space or period
        text = re.sub(r"(?<=s)'(?=[\s\.])", "’", text)
        # Replace if preceded by a space and followed by a letter
        text = re.sub(r"(?<=\s)'(?=[a-zA-Z])", "’", text)
    return text


def adhoc_transform(df):
    new_format_columns = {'extracted_label', 'quality_extracted_label', 'quality_methodology'}

    if not new_format_columns.issubset(df.columns):
        # Data is in the old format, adapting dataframe
        df['review_ds'] = df['last_review_ds']
        df['quality_methodology'] = 'Multi-review'
        df['extracted_label'] = 'not_used'
        df['quality_actor_id'] = 'not_used'
        df['quality_decision_data'] = 'not_used'
        df['quality_extracted_label'] = 'not_used'
    
    # Fix Json apostrophes
    df['decision_data'] = df['decision_data'].apply(fix_apostrophes)
    df['extracted_label'] = df['extracted_label'].apply(fix_apostrophes)
    df['quality_decision_data'] = df['quality_decision_data'].apply(fix_apostrophes)
    df['quality_extracted_label'] = df['quality_extracted_label'].apply(fix_apostrophes)

    return df


def transform(df, metadata):
    stats = {}
    stats["etl_module"] = "ADHOC-a01TR00000Eb8K1YAJ"
    stats["rows_before_transformation"] = len(df)

    df = adhoc_transform(df)

    # Force use_extracted:False
    mod_info = {
        "module": "UQD",
        "module_config": {
            "use_extracted": False,
            "quality_methodology": "multi",
            "excluded_labels": ["hallucination_example", "omit_example"],
            "binary_labels": []
        }
    }


    df, df_stats = uqd_transform(df, mod_info)
    stats["etl"] = df_stats

    stats["rows_after_transformation"] = len(df)
    return df, stats