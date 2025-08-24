import os
import pandas as pd
import re
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
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
TARGET_CBV2 = 0.64
TARGET_EB = 0.85

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
        if project_id == CBV2_PROJECT_ID:
            target_goal = TARGET_CBV2
        elif project_id == EB_PROJECT_ID:
            target_goal = TARGET_EB
        else:
            raise ValueError(f"Unknown project: {project_id}")
    else:
        raise ValueError(f"Unknown project: {project_id}")

    
    # Get the raw data folder for the project
    project_raw_folder = pu.get_project_folder(project_id, RAWDATA_BASE_FOLDER)["path"]
    reporting_week_str_dotted = f"WE {pd.to_datetime(reporting_week, errors='coerce').strftime('%Y.%m.%d')}"
    reporting_week_str = pd.to_datetime(reporting_week, errors='coerce').strftime('%Y-%m-%d')
    weekly_folder = os.path.join(project_raw_folder, reporting_week_str_dotted)

    client_dfs = {}
    
    ### Source: CLIENT OVERALL METRICS ###
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

        #rename cols
        df_overall.rename(columns={
            "accuracy": "workflow_score",
            "f1_score": "workflow_f1score",
            "precision": "workflow_precision",
            "recall": "workflow_recall"
        }, inplace=True)

        # week_ending | project_id | workflow | workflow_score | workflow_f1score | workflow_precision | workflow_recall | project_score | project_f1score | project_precision | project_recall
        df_overall = df_overall[["week_ending", "project_id", "workflow", "workflow_score", "workflow_f1score", "workflow_precision", "workflow_recall", "project_score", "project_f1score", "project_precision", "project_recall"]].copy()

        client_dfs['overall'] = df_overall


    ### Source: CLIENT RATER METRICS ###
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

        df_raters["workflow"] = df_raters["workflow"].str.replace(r'_v3$', '', regex=True)
            
        df_raters["tot_labels"] = df_raters["tp_count"] + df_raters["tn_count"] + df_raters["fp_count"] + df_raters["fn_count"]
        df_raters["correct_labels"] = df_raters["tp_count"] + df_raters["tn_count"]

        # Filter out rows with TPs+FPs+FNs = 0, handle NaNs
        cols = ['tp_count', 'fp_count', 'fn_count']
        df_raters[cols] = df_raters[cols].fillna(0)
        df_raters = df_raters[(df_raters["tp_count"] + df_raters["fp_count"] + df_raters["fn_count"]) > 0]

        # rename cols
        df_raters.rename(columns={
            "accuracy": "rater_label_score",
            "f1_score": "rater_label_f1score",
            "precision": "rater_label_precision",
            "recall": "rater_label_recall"
        }, inplace=True)

        score_cols = ["rater_label_score", "rater_label_f1score", "rater_label_precision", "rater_label_recall"]
        part_cols = ["week_ending", "project_id", "workflow", "rater_id"]
        means = (
            df_raters
            .groupby(part_cols)[score_cols]
            .transform("mean")
            .rename(columns={
                "rater_label_score": "rater_score",
                "rater_label_f1score": "rater_f1score",
                "rater_label_precision": "rater_precision",
                "rater_label_recall": "rater_recall",
            })
        )
        df_raters = pd.concat([df_raters, means], axis=1)

        # week_ending | project_id | workflow | rater_id | parent_label | tot_labels | correct_labels | tp_count | fp_count | fn_count | tn_count | rater_label_score | rater_label_f1score | rater_label_precision | rater_label_recall | rater_score | rater_f1score | rater_precision | rater_recall
        df_raters = df_raters[["week_ending", "project_id", "workflow", "rater_id", "parent_label", "tot_labels", "correct_labels", "tp_count", "fp_count", "fn_count", "tn_count", "rater_label_score", "rater_label_f1score", "rater_label_precision", "rater_label_recall", "rater_score", "rater_f1score", "rater_precision", "rater_recall"]]
        client_dfs['raters'] = df_raters

        ########### Now we build a df where we aggregate raters for each workflow and count how many are above target
        df_raters_temp = df_raters[["week_ending", "project_id", "workflow", "rater_id", "rater_f1score"]].copy().drop_duplicates()
        df_raters_temp["rater_is_above_target"] = (df_raters_temp["rater_f1score"] >= target_goal).astype(int)
        df_rater_summary = df_raters_temp.groupby(['week_ending', 'project_id', 'workflow']).agg(
            rater_count=('rater_id', 'nunique'),
            raters_above_target_f1=('rater_is_above_target', 'sum')
        ).reset_index()
        df_rater_summary['raters_above_target'] = df_rater_summary['raters_above_target_f1']

        # week_ending | project_id | workflow | rater_count | raters_above_target | raters_above_target_f1
        df_rater_summary = df_rater_summary[["week_ending", "project_id", "workflow", "rater_count", "raters_above_target", "raters_above_target_f1"]]
        client_dfs['rater_summary'] = df_rater_summary
    

    ### Source: OLAP Workflow ###
    olap_path = os.path.join(OLAP_BASE_FOLDER, project_id, reporting_week_str)
    olap_workflow_file = os.path.join(olap_path, f"{project_id}_{reporting_week_str}_audit_smr-workflow.csv")
    olap_rater_file = os.path.join(olap_path, f"{project_id}_{reporting_week_str}_audit_smr-rater-label.csv")

    df_olap_workflow_old = pd.read_csv(olap_workflow_file, low_memory=False, encoding='utf-8')
    df_olap_counts = df_olap_workflow_old[["project_id", "week_ending", "workflow", "auditor_count", "job_instances", "audited_instances", "audited_instances_f1"]]



    ### RE-CREATE OLAP EXPORTS ###
    olap_new_dfs = {}

    df_olap_overall_new = client_dfs["overall"] if "overall" in client_dfs else pd.DataFrame()
    # Attach OLAP counts
    df_olap_overall_new = df_olap_overall_new.merge(df_olap_counts, on=["project_id", "week_ending", "workflow"], how="left")
    # Attach rater_summary
    df_olap_overall_new = df_olap_overall_new.merge(client_dfs['rater_summary'], on=["project_id", "week_ending", "workflow"], how="left", suffixes=("", "_smr"))
    # Attach target goal
    df_olap_overall_new['target_goal'] = target_goal

    olap_new_dfs['workflow'] = df_olap_overall_new

    df_olap_rater_new = client_dfs['raters'] if "raters" in client_dfs else pd.DataFrame()
    olap_new_dfs['rater'] = df_olap_rater_new

    # Overwrite OLAP files
    olap_new_dfs['workflow'].to_csv(olap_workflow_file, index=False, encoding='utf-8')
    olap_new_dfs['rater'].to_csv(olap_rater_file, index=False, encoding='utf-8')
    #olap_new_dfs['workflow'].to_csv('workflow_temp.csv', index=False, encoding='utf-8')
    #olap_new_dfs['rater'].to_csv('rater_temp.csv', index=False, encoding='utf-8')
    
    print(f"Overwritten smr-workflow and smr-rater-label for project {project_id} - week {reporting_week_str}")

    return True



def overwrite_olap_all():
    we_dates = pu.generate_we_dates("2025-01-01")
    for week in we_dates:
        for project_id in [CBV2_PROJECT_ID, EB_PROJECT_ID]:

            reporting_week_str = pd.to_datetime(week, errors="coerce").strftime("%Y-%m-%d")
            olap_folder = os.path.join(OLAP_BASE_FOLDER, project_id, reporting_week_str)

            # verifica se esiste la cartella olap o se è vuota
            if not os.path.exists(olap_folder) or not os.listdir(olap_folder):
                continue

            print(f"Overwriting OLAP for project {project_id} - week {reporting_week_str}")
            overwrite_olap(project_id, reporting_week_str)



#overwrite_olap("a01Hs00001ocUa0IAE", "2025-08-15")
#overwrite_olap_all()