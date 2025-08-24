import os
import pandas as pd
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import pipeline_lib.config as cfg
import pipeline_lib.pipeline_utils as pu
from pipeline_lib.sql.queryrun import olap_query_run

OLAP_BASE_FOLDER = cfg.OLAP_EXPORT_DIR_PATH
TRANSFORMED_BASE_FOLDER = cfg.DATA_TRANSFORMED_DIR_PATH


# --- Setup Project List
PROJECT_MASTERFILE = cfg.PROJECT_INFO_FILE_PATH
project_list_df = pu.load_project_info(PROJECT_MASTERFILE, active_only=False)



#### Generate csv reports

def generate_olap_reports(project_id, project_base, reporting_week, target):
    reporting_week_str = pd.to_datetime(reporting_week, errors="coerce").strftime("%Y-%m-%d")
    olap_folder = os.path.join(OLAP_BASE_FOLDER, project_id, reporting_week_str)

    # verifica se esiste la cartella transformed
    transformed_folder = os.path.join(TRANSFORMED_BASE_FOLDER, project_id, reporting_week_str)
    if not os.path.exists(transformed_folder):
        return True
    
    os.makedirs(olap_folder, exist_ok=True)

    for query_name in ["smr-workflow"]:
        report_name = project_id + "_" + reporting_week_str + "_" + project_base + "_" + query_name + ".csv"
        
        report_df = olap_query_run(query_name, project_base, project_id, reporting_week, target)

        report_path = os.path.join(olap_folder, report_name)
        pu.save_df_to_filepath(report_df, report_path)
    
    # Override for CB and EB

    return True



def olap_backfill_audit():

    #df = project_list_df[(project_list_df['project_base'] == 'audit') & (project_list_df['project_status'].str.lower() == 'active')]
    df = project_list_df[(project_list_df['project_status'].str.lower() == 'active')]
    
    for _,row in df.iterrows():
        project_id = row['project_id']
        project_start_date = row['project_start_date']
        project_name = row['project_name']
        target = pu.get_project_target(project_id, project_list_df)
        project_base = pu.get_project_base(project_id, project_list_df)

        weeks_backfill = pu.generate_we_dates(project_start_date)
        print(f"Processing {project_id} ({project_name})")
        for week in weeks_backfill:
            reporting_week_str = week.strftime("%Y-%m-%d")
            
            outcome_success = generate_olap_reports(project_id, project_base, reporting_week_str, target)
            if not outcome_success:
                print(f"Failed to generate OLAP reports for {project_id} - {reporting_week_str}")
            else:
                print(f"{project_id} - {reporting_week_str} - OK")

olap_backfill_audit()