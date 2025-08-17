import os
import pandas as pd
import re
import pipeline_lib.config as cfg
import pipeline_lib.pipeline_utils as pu

RAWDATA_BASE_FOLDER = cfg.RAWDATA_ROOT_PATH
OLAP_BASE_FOLDER = cfg.OLAP_EXPORT_DIR_PATH

# --- Logger
import logging
logger = logging.getLogger(__name__)

# --- Setup Project List
PROJECT_MASTERFILE = cfg.PROJECT_INFO_FILE_PATH
project_list_df = pu.load_project_info(PROJECT_MASTERFILE, active_only=False)



CBV2_PROJECT_ID = "a01Hs00001ocUa0IAE"
EB_PROJECT_ID = "a01Hs00001ocUZgIAM"
OVERALL_FILENAME = "cvs_*_metrics_overview_*.csv"
RATER_FILENAME = "cvs_*_rater_level_metrics_*.csv"

rename_mapping = {
        "sample_ds": "job_date",
        "routing_name": "workflow",
        "label_category_name": "parent_label",
        "unweighted_accuracy": "accuracy",
        "unweighted_f1": "f1_score",
        "unweighted_precision": "precision",
        "unweighted_recall": "recall",
        "total_count": "label_count"
    }

def prepare_workflow_df(df):
    df.columns = df.columns.str.strip()
    df.rename(columns=rename_mapping, inplace=True)

    df['job_date'] = pd.to_datetime(df['job_date'], errors='coerce')
    job_dates = df['job_date'].dropna().unique()
    ordered_job_dates = sorted(job_dates, reverse=True)
    
    if not ordered_job_dates:
        print(f"No valid job dates found")
        return pd.DataFrame()
    df = df[df["job_date"] == ordered_job_dates[0]].copy()
    if len(job_dates) >= 1:
        ordered_job_dates = sorted(job_dates, reverse=True)
        # order by jobdate, newest first
        df = df[df["job_date"] == ordered_job_dates[0]].copy()
        
    # filter out parent_labels with "negative" in them
    df = df[~df["parent_label"].str.contains("negative", case=False, na=False)].copy()

    columns = ["job_date", "workflow", "parent_label", "accuracy", "f1_score", "precision", "recall"]
    df = df[columns].copy()
    return df


def prepare_rater_df(df):
    df.columns = df.columns.str.strip()
    df.rename(columns=rename_mapping, inplace=True)

    df['job_date'] = pd.to_datetime(df['job_date'], errors='coerce')
    job_dates = df['job_date'].dropna().unique()
    if len(job_dates) >= 1:
        ordered_job_dates = sorted(job_dates, reverse=True)
        # order by jobdate, newest first
        df = df[df["job_date"] == ordered_job_dates[0]].copy()

    columns = ["job_date", "workflow", "rater_id", "parent_label", \
                "label_count", "tp_count", "tn_count", "fp_count", "fn_count", \
                "accuracy", "f1_score", "precision", "recall"]
    df = df[columns].copy()
    return df





def overwrite_olap(project_id, reporting_week):
    reporting_week = pd.to_datetime(reporting_week, errors='coerce')

    if project_id == CBV2_PROJECT_ID or project_id == EB_PROJECT_ID:
        overall_filename = OVERALL_FILENAME
        rater_filename = RATER_FILENAME
        files_overall_pattern = f"{overall_filename.replace('*', '.*')}"
        files_rater_pattern = f"{rater_filename.replace('*', '.*')}"
    
    else:
        raise ValueError(f"Unknown project: {project_id}")

    
    # Get the raw data folder for the project
    project_raw_folder = pu.get_project_folder(project_id, RAWDATA_BASE_FOLDER)["path"]
    reporting_week_str_dotted = f"WE {pd.to_datetime(reporting_week, errors='coerce').strftime('%Y.%m.%d')}"
    reporting_week_str = pd.to_datetime(reporting_week, errors='coerce').strftime('%Y-%m-%d')
    weekly_folder = os.path.join(project_raw_folder, reporting_week_str_dotted)

    olap_dfs = {}
    
    ### OVERALL METRICS ###
    files_overall = [f for f in os.listdir(weekly_folder) if re.match(files_overall_pattern, f)]
    dfs_overall = []
    if files_overall:  
        print(f"Processing files: {files_overall}")
        
        for file in files_overall:
            file_path = os.path.join(weekly_folder, file)
            if not pu.has_at_least_one_data_row(file_path):
                print(f"Skipping empty file")
                continue
            df = pd.read_csv(file_path, low_memory=False, encoding='utf-8')
            if df.empty:
                continue
            df = prepare_workflow_df(df)
            dfs_overall.append(df)

    if dfs_overall:
        df_overall = pd.concat(dfs_overall, ignore_index=True)
        df_overall["week_ending"] = reporting_week_str
        df_overall["project_id"] = project_id
        df_overall.drop(columns=["job_date"], inplace=True)

        # se la colonna workflow contiene una stringa del tipo cb_exagg_*, sostituisco in cb_withhold_*
        df_overall["workflow"] = df_overall["workflow"].str.replace(r'^cb_exagg_', 'cb_withhold_', regex=True)
                
        # rimuovo suffisso _v3 dalla colonna workflow
        df_overall["workflow"] = df_overall["workflow"].str.replace(r'_v3$', '', regex=True)

        df_overall.drop(columns=["parent_label"], inplace=True)

        df_overall = df_overall.groupby(["project_id", "week_ending", "workflow"]).mean().reset_index()

        score = df_overall.loc[df_overall['workflow'] == 'combined_routing', 'accuracy'].mean()
        f1 = df_overall.loc[df_overall['workflow'] == 'combined_routing', 'f1_score'].mean()
        prec = df_overall.loc[df_overall['workflow'] == 'combined_routing', 'precision'].mean()
        recll = df_overall.loc[df_overall['workflow'] == 'combined_routing', 'recall'].mean()

        #togli la riga dove workflow è combined_routing
        df_overall = df_overall[df_overall["workflow"] != "combined_routing"].copy()

        df_overall['project_score'] = score
        df_overall['project_f1score'] = f1
        df_overall['project_precision'] = prec
        df_overall['project_recall'] = recll

        olap_dfs['overall'] = df_overall


    ### RATER METRICS ###
    files_raters = [f for f in os.listdir(weekly_folder) if re.match(files_rater_pattern, f)]
    dfs_raters = []
    if files_raters:
        print(f"Processing files: {files_raters}")
        for file in files_raters:
            file_path = os.path.join(weekly_folder, file)
            if not pu.has_at_least_one_data_row(file_path):
                print(f"Skipping empty file")
                continue
            df = pd.read_csv(file_path, low_memory=False, encoding='utf-8')
            if df.empty:
                continue
            df = prepare_rater_df(df)
            dfs_raters.append(df)

    if dfs_raters:
        df_raters = pd.concat(dfs_raters, ignore_index=True)
            
        if project_id == CBV2_PROJECT_ID:
            df_raters['parent_label'] = df_raters['parent_label'].replace(to_replace=r'.*exagg.*', value='exaggeration', regex=True)
            df_raters['parent_label'] = df_raters['parent_label'].replace(to_replace=r'.*withhold.*', value='withhold', regex=True)
            df_raters["workflow"] = df_raters["workflow"].str.replace(r'^cb_exagg_', 'cb_withhold_', regex=True)

        df_raters["week_ending"] = reporting_week_str
        df_raters["project_id"] = project_id
            
        df_raters["tot_labels"] = df_raters["tp_count"] + df_raters["tn_count"] + df_raters["fp_count"] + df_raters["fn_count"]
        df_raters["correct_labels"] = df_raters["tp_count"] + df_raters["tn_count"]

        # Filter out contributors with TPs+FPs+FNs = 0
        #df_raters = df_raters.groupby(['rater_id', 'workflow', 'job_date']).filter(
        #    lambda g: (g[['tp_count', 'fp_count', 'fn_count']].sum().sum()) != 0
        #)

                
        df_raters["workflow"] = df_raters["workflow"].str.replace(r'_v3$', '', regex=True)

        olap_dfs['raters'] = df_raters



    ### OLAP EXPORT ###
    olap_path = os.path.join(OLAP_BASE_FOLDER, project_id, reporting_week_str)
    df_overall = olap_dfs["overall"] if "overall" in olap_dfs else pd.DataFrame()
    if not df_overall.empty:
        # OVERALL: merge with olap export Workflow
        olap_workflow_file = os.path.join(olap_path, f"{project_id}_{reporting_week_str}_audit_smr-workflow.csv")
        df_olap_workflow = pd.read_csv(olap_workflow_file, low_memory=False, encoding='utf-8')

        #merge df olap workflow with overall df on project_id, week_ending, workflow (inner join), keep the same structure of the olap workflow df: replace columns workflow_score with overall df accuracy, workflow_f1score with overall df f1_score, workflow_precision with overall df precision, workflow_recall with overall df recall

        df_olap_workflow = df_olap_workflow.merge(df_overall, on=["project_id", "week_ending", "workflow"], suffixes=("", "_cvs"), how="inner")
        #df_olap_workflow.to_csv('test_olap_wf.csv', index=False, encoding='utf-8')
        df_olap_workflow["workflow_score"] = df_olap_workflow["accuracy"]
        df_olap_workflow["workflow_f1score"] = df_olap_workflow["f1_score"]
        df_olap_workflow["workflow_precision"] = df_olap_workflow["precision"]
        df_olap_workflow["workflow_recall"] = df_olap_workflow["recall"]

        df_olap_workflow["project_score"] = df_olap_workflow["project_score_cvs"]
        df_olap_workflow["project_f1score"] = df_olap_workflow["project_f1score_cvs"]
        df_olap_workflow["project_precision"] = df_olap_workflow["project_precision_cvs"]
        df_olap_workflow["project_recall"] = df_olap_workflow["project_recall_cvs"]
        df_olap_workflow.drop(columns=["accuracy", "f1_score", "precision", "recall", "project_score_cvs", "project_f1score_cvs", "project_precision_cvs", "project_recall_cvs"], inplace=True)

        reordered_columns = [
            "project_id", "week_ending", "workflow", "rater_count", "auditor_count", "job_instances", "audited_instances",
            "target_goal", "raters_above_target", "raters_above_target_f1", 
            "workflow_score", "workflow_f1score", "workflow_precision", "workflow_recall", 
            "project_score", "project_f1score", "project_precision", "project_recall"
        ]
        df_olap_workflow = df_olap_workflow[reordered_columns]
        df_olap_workflow.to_csv(olap_workflow_file, index=False, encoding='utf-8')

    df_raters = olap_dfs["raters"] if "raters" in olap_dfs else pd.DataFrame()
    if not df_raters.empty:
        # RATER: overwrite olap export Rater
        olap_rater_file = os.path.join(olap_path, f"{project_id}_{reporting_week_str}_audit_smr-rater-label.csv")
        df_raters.to_csv('test_raters.csv', index=True)

        group_cols = ['week_ending', 'project_id', 'rater_id', 'parent_label']
        df_raters['rater_score']        = df_raters.groupby(group_cols)['accuracy'].transform('mean')
        df_raters['rater_f1score']      = df_raters.groupby(group_cols)['f1_score'].transform('mean')
        df_raters['rater_precision']    = df_raters.groupby(group_cols)['precision'].transform('mean')
        df_raters['rater_recall']       = df_raters.groupby(group_cols)['recall'].transform('mean')

        renamed_cols = {
                'accuracy': 'rater_label_score',
                'f1_score': 'rater_label_f1score',
                'precision': 'rater_label_precision',
                'recall': 'rater_label_recall'
        }
        df_raters.rename(columns=renamed_cols, inplace=True)
        # sort by rater_id, parent_label
        df_raters.sort_values(by=['rater_id', 'parent_label'], inplace=True)
        df_olap_raters = df_raters.copy()

        reordered_columns = [
            "week_ending", "project_id", "workflow", "rater_id", "parent_label", 
            "tot_labels", "correct_labels", "tp_count", "tn_count", "fp_count", "fn_count", 
            "rater_label_score", "rater_label_f1score", "rater_label_precision", "rater_label_recall", 
            "rater_score", "rater_f1score", "rater_precision", "rater_recall"
        ]
        df_olap_raters = df_olap_raters[reordered_columns]
        df_olap_raters.to_csv(olap_rater_file, index=False, encoding='utf-8')

    print(f"Overwritten smr-workflow and smr-rater-label for project {project_id} - week {reporting_week_str}")

    return True



def overwrite_olap_all():
    we_dates = pu.generate_we_dates("2025-01-01")
    for week in we_dates:
        for project_id in [CBV2_PROJECT_ID, EB_PROJECT_ID]:
            print(f"Overwriting OLAP for project {project_id} - week {week}")
            overwrite_olap(project_id, week)



#overwrite_olap("CB", "2025-08-08")