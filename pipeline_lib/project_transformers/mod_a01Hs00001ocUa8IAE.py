###################
# Project 'Conness'
###################

import pandas as pd
import numpy as np
from pipeline_lib.project_transformers import transformer_utils
import logging

# --- Setup logger
logger = logging.getLogger('pipeline.transform_modules')




def adhoc_transform(df, binary_labels, excluded_labels):
    df = df[df['q1_ad_load'] == 'yes']

    if 'reason_other' not in df.columns:
        df['reason_other'] = 'N/A'

    df = df.rename(columns={
        'review_ds': 'submission_date',
        'extracted_label': 'contributor_relevance',
        '_worker_id': 'auditor_id',
        'actor_id': 'rater_id'
    })

    df['contributor_cannot_decide_reason'] = np.where(
          (df['contributor_relevance'] == 'can_not_rate') & (df['reason_other'] == 'N/A'),
         'Other',
          df['reason_other']
    )

    df['auditor_relevance'] = np.where(
       (df['q2_contributor_correct'] == 'yes'),
        df['contributor_relevance'],
        df['q3_choice']
    )

    df['auditor_cannot_decide_reason'] = np.where(
        (df['q2_contributor_correct'] == 'yes') & (df['q3_choice'].fillna('') == '') & (df['q4_cannot_decide'].fillna('') == ''),
        df['contributor_cannot_decide_reason'],
        df['q4_cannot_decide']
    )

    replace_dict = {
        'rating_v1_0_no': 'Not Related',
        'rating_v1_1_yes': 'Somewhat Related',
        'rating_v1_2_yes': 'Relevant',
        'prod_language': 'Cannot Decide_Product Language',
        'ad_language': 'Cannot Decide_Ad Language',
        'ad_unknown': 'Cannot Decide_Ad Unknown',
        'prod_unknown': 'Cannot Decide_Product Unknown',
        'prod_issue': 'Cannot Decide_Product Issue',
        'ad_issue': 'Cannot Decide_Ad Issue',
        'can_not_rate': 'Cannot Decide',
        'no_rate_other_reason': 'Cannot Decide_Other',
        "Doesn't understand the language the product uses": 'Product Language',
        "Doesn't understand the language the ad uses": 'Ad Language',
        "Don't know what the ad is promoting": 'Ad Unknown',
        "Don't know what the product is promoting": 'Product Unknown',
        'Product is not rendering correctly': 'Product Issue',
        'Ad is not rendering correctly': 'Ad Issue',
        'N/A': ''
    }
    columns_to_replace = [
        'contributor_relevance',
        'auditor_relevance',
        'contributor_cannot_decide_reason',
        'auditor_cannot_decide_reason'
    ]
    for col in columns_to_replace:
        if col in df.columns:
            df[col] = df[col].replace(replace_dict)

    df_main = df[['submission_date', 'job_id', 'rater_id', 'auditor_id', 'contributor_relevance', 'contributor_cannot_decide_reason', 'auditor_relevance', 'auditor_cannot_decide_reason']]

    df_rater = df_main[['submission_date', 'job_id', 'rater_id', 'contributor_relevance', 'contributor_cannot_decide_reason']].drop_duplicates()
    df_rater['is_audit'] = 0
    df_rater = df_rater.rename(columns={
        'rater_id': 'actor_id',
        'contributor_relevance': 'relevance',
        'contributor_cannot_decide_reason': 'cannot_decide_reason'
    })
    # unpivoting
    df_rater_melted = pd.melt(
        df_rater,
        id_vars=['submission_date', 'job_id', 'actor_id', 'is_audit'],
        value_vars=['relevance', 'cannot_decide_reason'],
        var_name='parent_label',
        value_name='response_data'
    )
    df_rater_melted = df_rater_melted.drop_duplicates()


    df_auditor = df_main[['submission_date', 'job_id', 'auditor_id', 'auditor_relevance', 'auditor_cannot_decide_reason']].drop_duplicates()
    df_auditor['is_audit'] = 1
    df_auditor = df_auditor.rename(columns={
        'auditor_id': 'actor_id',
        'auditor_relevance': 'relevance',
        'auditor_cannot_decide_reason': 'cannot_decide_reason'
    })

    # unpivoting
    df_auditor_melted = pd.melt(
        df_auditor,
        id_vars=['submission_date', 'job_id', 'actor_id', 'is_audit'],
        value_vars=['relevance', 'cannot_decide_reason'],
        var_name='parent_label',
        value_name='response_data'
    )
    df_auditor_melted = df_auditor_melted.drop_duplicates()

    df_combined = pd.concat([df_rater_melted, df_auditor_melted], ignore_index=True)

    df_combined['response_data'] = df_combined['response_data'].replace(['', 'na', 'null', 'NA', 'NaN'], pd.NA).fillna('not_chosen')
    df_combined['response_data'] = df_combined['response_data'].str.removeprefix('Cannot Decide_')

    return df_combined



def transform(df, metadata):
    stats = {}
    stats["etl_module"] = "ADHOC-a01Hs00001ocUa8IAE"
    stats["rows_before_transformation"] = len(df)

    binary_labels = transformer_utils.get_binary_labels(metadata)
    excluded_labels = transformer_utils.get_excluded_labels(metadata)
    use_extracted = transformer_utils.get_use_extracted(metadata)
    quality_methods = transformer_utils.get_quality_methods(metadata)

    df = adhoc_transform(df, binary_labels, excluded_labels)
    stats["rows_after_transformation"] = len(df)

    df = transformer_utils.enrich_dataframe_with_metadata(df, metadata)

    return df, stats