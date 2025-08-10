import os
import pandas as pd
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import pipeline_lib.config as cfg
import pipeline_lib.pipeline_utils as pu
from pipeline_lib.queues import TransformationQueueManager
from pipeline_lib.sql.queryrun import olap_query_run

OLAP_BASE_FOLDER = cfg.OLAP_EXPORT_DIR_PATH
TRANSFORMED_BASE_FOLDER = cfg.DATA_TRANSFORMED_DIR_PATH


# --- Setup Project List
PROJECT_MASTERFILE = cfg.PROJECT_INFO_FILE_PATH
project_list_df = pu.load_project_info(PROJECT_MASTERFILE, active_only=True)



#### Generate csv reports

def generate_olap_reports(project_id, project_base, reporting_week, target):
    reporting_week_str = pd.to_datetime(reporting_week, errors="coerce").strftime("%Y-%m-%d")
    olap_folder = os.path.join(OLAP_BASE_FOLDER, project_id, reporting_week_str)
    os.makedirs(olap_folder, exist_ok=True)
    
    #for query_name in ["smr-workflow", "smr-rater-label", "smr-job-label", "smr-error-contribution", "dmp-job-incorrect"]:
    for query_name in ["smr-error-contribution"]:
        report_name = project_id + "_" + reporting_week_str + "_" + project_base + "_" + query_name + ".csv"
        
        report_df = olap_query_run(query_name, project_base, project_id, reporting_week, target)

        report_path = os.path.join(olap_folder, report_name)
        pu.save_df_to_filepath(report_df, report_path)
    
    # Override for CB and EB

    return True



def olap_sync_override():
    print(f"[INFO] OLAP Backfill Started")
    project_count = 0
    for _, row in project_list_df.iterrows():
        project_error_count = 0
        project_success_count = 0

        project_id = row['project_id']
        project_name = row['project_name']
        project_start_date = row['project_start_date']
        project_target = pu.get_project_target(project_id, project_list_df)
        project_base = pu.get_project_base(project_id, project_list_df)
        project_week_dates = pu.generate_we_dates(project_start_date)

        for week_date in project_week_dates:
            # check if folder exists in transformed data
            week_folder_name = week_date.strftime("%Y-%m-%d")
            week_transformed_path = os.path.join(TRANSFORMED_BASE_FOLDER, project_id, week_folder_name)
            print(f"Project: {project_name} | ID: {project_id} | Week: {week_folder_name}")

            if os.path.exists(week_transformed_path):
                outcome_success = generate_olap_reports(project_id, project_base, week_folder_name, project_target)
                if outcome_success:
                    #print("Olap Sync SUCCESS!")
                    project_success_count += 1
                else:
                    #print("Olap Sync Failed!")
                    project_error_count += 1
            #else:
            #    print(f"Path doesnt exist: {week_transformed_path}")
            #    project_error_count += 1
        
        print(f"Project {project_name} ({project_id}) - Success: {project_success_count}, Errors: {project_error_count}")
        project_count += 1

    print(f"[INFO] OLAP Backfill Ended ({project_count} projects processed)")


olap_sync_override()