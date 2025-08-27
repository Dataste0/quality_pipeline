import os
import pandas as pd

import pipeline_lib.config as cfg
import pipeline_lib.pipeline_utils as pu
from pipeline_lib.queues import TransformationQueueManager
from pipeline_lib.sql.queryrun import olap_query_run
from pipeline_lib.baits_exception import overwrite_olap

OLAP_BASE_FOLDER = cfg.OLAP_EXPORT_DIR_PATH

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
    
    for query_name in ["smr-workflow", "smr-rater-label", "smr-job-label", "smr-error-contribution", "dmp-job-incorrect"]:
        report_name = project_id + "_" + reporting_week_str + "_" + project_base + "_" + query_name + ".csv"
        
        report_df = olap_query_run(query_name, project_base, project_id, reporting_week, target)

        report_path = os.path.join(olap_folder, report_name)
        pu.save_df_to_filepath(report_df, report_path)
    

    # Override for CB and EB
    CBV2_PROJECT_ID = "a01Hs00001ocUa0IAE"
    EB_PROJECT_ID = "a01Hs00001ocUZgIAM"
    if project_id == CBV2_PROJECT_ID or project_id == EB_PROJECT_ID:
        overwrite_olap(project_id, reporting_week_str)

    return True



def olap_sync():
    total_olap_sync_items = transformation_queue.count(status="olap_sync_ready")

    print(f"[INFO] OLAP Sync Phase Started ({total_olap_sync_items} items)")
    logger.info(f"OLAP Sync Phase Started ({total_olap_sync_items} items)")

    success_count = 0
   
    for i in range(total_olap_sync_items):
        olap_sync_item = transformation_queue.pop(mode="olap_sync_ready")
        if not olap_sync_item:
            logger.warning(f"Queue empty before expected!")
            break
        olap_sync_item_id = olap_sync_item['item_id']
        print(f"[{i+1}/{total_olap_sync_items}] Processing ID {olap_sync_item_id}: {olap_sync_item['project_id']} | {olap_sync_item['project_name']} // {olap_sync_item['data_week']}")

        project_id = olap_sync_item['project_id']
        target = pu.get_project_target(project_id, project_list_df)
        project_base = pu.get_project_base(project_id, project_list_df)

        #print(f"\nTarget: {target} - Project Base: {project_base}")
        
        reporting_week = olap_sync_item['data_week']
        outcome_success = generate_olap_reports(project_id, project_base, reporting_week, target)
        if outcome_success:
            #print("Olap Sync SUCCESS!")
            transformation_queue.mark_olap_synced(olap_sync_item_id)
            success_count += 1
        else:
            print("Olap Sync Failed!")


    print(f"[INFO] OLAP Sync Phase Ended ({total_olap_sync_items} items synced)")
    logger.info(f"OLAP Sync Phase Ended ({total_olap_sync_items} items synced)")

    return success_count