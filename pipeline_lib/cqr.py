import os
import pandas as pd
import pipeline_lib.pipeline_utils as pu
import pipeline_lib.config as cfg

# --- Logger
import logging
logger = logging.getLogger(__name__)


# --- Setup Project List
PROJECT_MASTERFILE = cfg.PROJECT_INFO_FILE_PATH
project_list_df = pu.load_project_info(PROJECT_MASTERFILE, active_only=True)

# --- CQR Path
CQR_PATH = cfg.CQR_ROOT_PATH
EXPORT_PATH = cfg.OLAP_EXPORT_DIR_PATH


def cqr():
    logger.info(f"CQR Phase Started")
    print("[INFO] CQR Phase Started")

    if not os.path.exists(CQR_PATH):
        logger.error(f"Raw data root folder not found: {CQR_PATH}. Aborting.")
        return None

    CQRs = []

    # Get Week Ending Dates
    current_date = pd.to_datetime("today")
    previous_weekending = pu.get_friday_of_week(current_date) - pd.Timedelta(days=7)

    if pd.notna(previous_weekending):
        previous_weekending_str = previous_weekending.strftime("%Y-%m-%d")
        print(f"Processing CQR for week ending: {previous_weekending_str}")
    else:
        raise ValueError(f"Cannot determine previous week ending date from current date: {current_date}")

    project_df = project_list_df

    # Iterate Projects on Project masterfile
    processed_count = 0
    skipped_count = 0
    total_projects = len(project_df)

    for i, (_, row) in enumerate(project_df.iterrows(), start=1):
        project_id = row["project_id"]
        project_name = row["project_name"]
        print(f"Processing project {i}/{total_projects}: {project_id} ({project_name})")
        
        project_metadata = pu.get_project_metadata(project_id, project_list_df)
        project_base = project_metadata.get("project_base", None)
        if project_base is None:
            skipped_count += 1
            logger.warning(f"Project base not found for project {project_id}. Skipping.")
            continue
        project_target = project_metadata.get("project_target", None)
        project_metric = project_metadata.get("project_metric", "").lower()
        
        # Get project folder
        project_folder = os.path.join(EXPORT_PATH, project_id, previous_weekending_str)
        if not os.path.exists(project_folder):
            skipped_count += 1
            logger.warning(f"Export Project-week folder not found for project {project_id} at {project_folder}. Skipping.")
            continue

        # Get rater export file (file ending with _smr-rater-label.csv)
        rater_export_file_path = os.path.join(project_folder, f"{project_id}_{previous_weekending_str}_{project_base}_smr-rater-label.csv")
        if not os.path.exists(rater_export_file_path):
            logger.warning(f"Rater export file not found for project {project_id} at {rater_export_file_path}. Skipping.")
            continue

        # Load rater export file
        try:
            rater_export_df = pd.read_csv(rater_export_file_path)
        except Exception as e:
            skipped_count += 1
            logger.error(f"Error loading rater export file for project {project_id} at {rater_export_file_path}: {e}. Skipping.")
            continue


        # Append ' to rater_id to preserve leading zeros in Excel
        rater_export_df["rater_id"] = (
            rater_export_df["rater_id"]
            .astype("string")
            .where(rater_export_df["rater_id"].notna(), None)
            .map(lambda x: f"'{x}" if x is not None else x)
        )

        # consider n as separator
        rater_export_df['project_id_main'] = rater_export_df['project_id'].str.split('n').str[0]
        rater_export_df['segment'] = rater_export_df['project_id'].str.split('n').str[1]
        # Fill segments with '1' if missing
        rater_export_df['segment'] = rater_export_df['segment'].fillna('1')

        # remove project_id col and replace with project_id_main
        rater_export_df = rater_export_df.drop(columns=['project_id'])
        rater_export_df = rater_export_df.rename(columns={'project_id_main': 'project_id'})

        # drop columns not used
        cols_to_drop = ['parent_label', 'rater_label_score', 'rater_label_f1score', 'rater_label_precision', 'rater_label_recall']
        rater_export_df = rater_export_df.drop(columns=cols_to_drop)

        # groupby week_ending, project_id, segment, rater_id: sum tot_labels, correct_labels, tp_count, fp_count, tn_count, fn_count, average columns rater_score, rater_f1score, rater_precision, rater_recall
        aggregation_functions = {
            'tot_labels': 'sum',
            'correct_labels': 'sum',
            'tp_count': 'sum',
            'fp_count': 'sum',
            'tn_count': 'sum',
            'fn_count': 'sum',
            'rater_score': 'mean',
            'rater_f1score': 'mean',
            'rater_precision': 'mean',
            'rater_recall': 'mean'
        }
        rater_export_df = rater_export_df.groupby(['week_ending', 'project_id', 'segment', 'rater_id'], as_index=False).aggregate(aggregation_functions)

        if project_metric == "agreement":
            rater_export_df['rater_agreement'] = rater_export_df['rater_score']
            rater_export_df['rater_accuracy'] = ""
        else:
            rater_export_df['rater_accuracy'] = rater_export_df['rater_score']
            rater_export_df['rater_agreement'] = ""
        rater_export_df = rater_export_df.drop(columns=['rater_score'])

        # add target goal
        rater_export_df['target_goal'] = project_target

        # add project_name
        rater_export_df.insert(3, 'project_name', project_name)
        
        # Save this table in CQRs list
        CQRs.append(rater_export_df)
        processed_count += 1

    print(f"Processed {processed_count} projects (skipped {skipped_count}).")

    # Concatenate all CQRs
    if CQRs:
        CQR_df = pd.concat(CQRs, ignore_index=True)

        # Make sure all columns are present, if not fill with "" and keep order
        required_columns = [
            "week_ending", "project_id", "project_name", "segment", "rater_id", "tot_labels", "correct_labels",
            "tp_count", "fp_count", "tn_count", "fn_count", "rater_accuracy", "rater_agreement",
            "rater_f1score", "rater_precision", "rater_recall", "target_goal"
        ]
        for col in required_columns:
            if col not in CQR_df.columns:
                CQR_df[col] = ""

        # Bulk rename columns
        column_renames = {
            "week_ending": "Week Ending",
            "project_id": "Project ID",
            "project_name": "Project Name",
            "segment": "Segment",
            "rater_id": "Contributor ID",
            "tot_labels": "Total Labels",
            "correct_labels": "Correct Labels",
            "tp_count": "True Positives",
            "fp_count": "False Positives",
            "tn_count": "True Negatives",
            "fn_count": "False Negatives",
            "rater_accuracy": "Accuracy",
            "rater_agreement": "Agreement",
            "rater_f1score": "F1 Score",
            "rater_precision": "Precision",
            "rater_recall": "Recall",
            "target_goal": "Target Goal"
        }
        CQR_df = CQR_df.rename(columns=column_renames)
        
        # Save CQR_df to CQR_PATH with filename cqr_<previous_weekending_str>.xlsx
        
        cqr_file_path = os.path.join(CQR_PATH, f"cqr_{previous_weekending_str}.xlsx")
        try:
            CQR_df.to_excel(cqr_file_path, index=False)
            logger.info(f"CQR file saved at {cqr_file_path}")
            print(f"[INFO] CQR file saved at {cqr_file_path}")
        except Exception as e:
            logger.error(f"Error saving CQR file at {cqr_file_path}: {e}")
    else:
        logger.warning("No CQR data to save.")
        print("[WARNING] No CQR data to save.")

    logger.info(f"CQR Phase Ended")
    print("[INFO] CQR Phase Ended")
    return True
