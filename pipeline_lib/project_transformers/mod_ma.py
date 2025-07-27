############################
#     DEPRECATED
############################


import pandas as pd
import logging
from pipeline_lib.project_transformers import transformer_utils


# --- Setup logger
logger = logging.getLogger('pipeline.transform_modules')



def MA_unpivot_actor_ids(df, is_correct=True):
    df_long = pd.melt(
        df,
        id_vars=['submission_date', 'workflow', 'job_id'],
        value_vars=['rater_id', 'auditor_id'],
        var_name='actor_role',
        value_name='actor_id'
    )
    df_long['actor_role'] = df_long['actor_role'].str.replace('_id', '', regex=False)

    df_long['parent_label'] = 'default_label'
    if is_correct:
        df_long['response_data'] = True
    else:
        df_long['response_data'] = df_long['actor_role'] == 'auditor'

    df_long['is_audit'] = df_long['actor_role'].map(lambda x: '1' if x == 'auditor' else '0')
    return df_long


def MA_transform(df):

    # Rename columns
    df.columns = [col.strip().lower().replace(' ', '_') for col in df.columns]

    # Keep selected cols
    selected_columns = ['review', 'queue', 'job', 'reviewer_id', 'auditor_id', 'outcome', 'acknowledgement', 'dispute_status']
    df = df[selected_columns]

    # Correct jobs
    df_correct = df[df['outcome'] == 'Yes'].copy()

    # Incorrect jobs
    conditions = (
        (df['outcome'] == 'No') &
        ((df['acknowledgement'] == '#DISAGREE') | (df['acknowledgement'] == '#UNDISPUTED')) &
        (df['dispute_status'] == '#Agrees')
    )
    df_incorrect = df[conditions].copy()

    # Remove unused columns
    df_correct = df_correct[['review', 'queue', 'job', 'reviewer_id', 'auditor_id']]
    df_incorrect = df_incorrect[['review', 'queue', 'job', 'reviewer_id', 'auditor_id']]

    # Cols rename
    columns_dict = {
        'review': 'submission_date',
        'job': 'job_id',
        'reviewer_id': 'rater_id',
        'queue': 'workflow'
    }
    df_correct = df_correct.rename(columns=columns_dict)
    df_incorrect = df_incorrect.rename(columns=columns_dict)

    # Unpivoting
    df_correct_long = MA_unpivot_actor_ids(df_correct, is_correct=True)
    df_incorrect_long = MA_unpivot_actor_ids(df_incorrect, is_correct=False)

    # Concat correct + incorrect dfs
    df = pd.concat([df_correct_long, df_incorrect_long], ignore_index=True)
    df = df.drop(columns=['actor_role'])
    df['job_id'] = df['job_id'].astype(str).str.replace('#', '', regex=False)

    # REMOVE DUPLICATES (Auditor has duplicated rows after the unpivoting)
    # order by job_id, actor_id and submission_date DESC (most recent first)
    df = df.sort_values(by=['job_id', 'actor_id', 'submission_date'], ascending=[True, True, False])

    # Keep only most recent row for each tuple (job_id, actor_id)
    df = df.drop_duplicates(subset=['job_id', 'actor_id'], keep='first')
    df = df.reset_index(drop=True)

    return df



def transform(df, metadata):
    """
    Entry point for MA projects. Extracts parameters from metadata and calls MA_transform.
    """

    df = MA_transform(df)
    df = transformer_utils.enrich_dataframe_with_metadata(df, metadata)

    return df