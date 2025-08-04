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

# --- Setup queues
TRANSFORMATION_QUEUE_FILE = cfg.QUEUE_TRANSFORMATION_FILE_PATH
transformation_queue = TransformationQueueManager(TRANSFORMATION_QUEUE_FILE)

# --- Logger
import logging
logger = logging.getLogger(__name__)

# --- Setup Project List
PROJECT_MASTERFILE = cfg.PROJECT_INFO_FILE_PATH
project_list_df = pu.load_project_info(PROJECT_MASTERFILE, active_only=False)



#### Generate csv reports

def generate_olap_reports(project_id, project_base, reporting_week, target):
    reporting_week_str = pd.to_datetime(reporting_week, errors="coerce").strftime("%Y-%m-%d")
    olap_folder = os.path.join(OLAP_BASE_FOLDER, project_id, reporting_week_str)
    os.makedirs(olap_folder, exist_ok=True)
    
    for query_name in ["smr-error-contribution"]:
        report_name = project_id + "_" + reporting_week_str + "_" + project_base + "_" + query_name + ".csv"
        
        report_df = olap_query_run(query_name, project_base, project_id, reporting_week, target)

        report_path = os.path.join(olap_folder, report_name)
        pu.save_df_to_filepath(report_df, report_path)

    return True



def olap_sync_override():
    df = pu.load_df_from_filepath('C:\\Users\\steco\\Appen\\Appen Quality Team - Dashboard\\Data_Process\\transformation_queue.csv')
    total_olap_sync_items = len(df)

    print(f"[INFO] OLAP Sync Phase Started ({total_olap_sync_items} items)")
    logger.info(f"OLAP Sync Phase Started ({total_olap_sync_items} items)")
   
    i=0
    for _, row in df.iterrows():
        
        olap_sync_item_id = row['item_id']
        print(f"[{i+1}/{total_olap_sync_items}] Processing ID {olap_sync_item_id}: {row['project_id']} | {row['project_name']} // {row['data_week']}")

        project_id = row['project_id']
        target = pu.get_project_target(project_id, project_list_df)
        project_base = pu.get_project_base(project_id, project_list_df)

        #print(f"\nTarget: {target} - Project Base: {project_base}")
        
        reporting_week = pd.to_datetime(row['data_week'], errors="coerce").strftime("%Y-%m-%d")
        #verifica se esiste la week dir
        weekdir_path = os.path.join(TRANSFORMED_BASE_FOLDER, project_id, reporting_week)
        if os.path.exists(weekdir_path):
            outcome_success = generate_olap_reports(project_id, project_base, reporting_week, target)
            if outcome_success:
                print("Olap Sync SUCCESS!")
            else:
                print("Olap Sync Failed!")
        else:
            print(f"Path doesnt exist: {weekdir_path}")
        i += 1


    print(f"[INFO] OLAP Sync Phase Ended ({total_olap_sync_items} items synced)")
    logger.info(f"OLAP Sync Phase Ended ({total_olap_sync_items} items synced)")   


olap_sync_override()