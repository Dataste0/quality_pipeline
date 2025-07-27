import logging
import os
import pandas as pd
import ast

import pipeline_lib.config as cfg
import pipeline_lib.pipeline_utils as pu
from pipeline_lib.queues import TransformationQueueManager
from pipeline_lib.etl.queryrun import query_run

OLAP_BASE_FOLDER = cfg.OLAP_EXPORT_DIR_PATH

TRANSFORMATION_QUEUE_FILE = cfg.QUEUE_TRANSFORMATION_FILE_PATH

PROJECT_MASTERFILE = cfg.PROJECT_INFO_FILE_PATH

# --- Setup queues
transformation_queue = TransformationQueueManager(TRANSFORMATION_QUEUE_FILE)

# --- Setup logger
logger = logging.getLogger('pipeline.olap')

# --- Setup Project List
project_list_df = pu.load_project_info(PROJECT_MASTERFILE, active_only=False)


#### Generate csv reports

def generate_olap_reports(project_id, week_ending, target, methodology):
    week_ending = pd.to_datetime(week_ending, dayfirst=False, errors='raise').strftime("%Y-%m-%d")
    olap_folder = os.path.join(OLAP_BASE_FOLDER, "Project_" + project_id, week_ending)
    os.makedirs(olap_folder, exist_ok=True)
    
    for rep in ["smr-workflow", "smr-rater-label", "smr-job-label", "smr-error-contribution", "job-incorrect"]:
        report_type = rep
        report_name = project_id + "_" + week_ending + "_" + methodology + "_" + report_type + ".csv"
        report_df = query_run(report_type, project_id, week_ending, target, methodology)
        report_path = os.path.join(olap_folder, report_name)
        pu.save_df_to_filepath(report_df, report_path)

    return True



def olap_sync():
    total_olap_sync_items = transformation_queue.count(status="olap_sync_ready")

    print(f"[INFO] OLAP Sync Phase Started ({total_olap_sync_items} items)")
    logger.info(f"OLAP Sync Phase Started ({total_olap_sync_items} items)")
   
    for i in range(total_olap_sync_items):
        olap_sync_item = transformation_queue.pop(mode="olap_sync_ready")
        if not olap_sync_item:
            logger.warning(f"Queue empty before expected!")
            break
        olap_sync_item_id = olap_sync_item['item_id']
        print(f"[{i+1}/{total_olap_sync_items}] Processing ID {olap_sync_item_id}: {olap_sync_item['project_id']} | {olap_sync_item['project_name']} // {olap_sync_item['content_weeks']}")

        project_id = olap_sync_item['project_id']
        target = pu.get_project_target(project_id, project_list_df)
        methodology = pu.get_project_methodology(project_id, project_list_df)

        #print(f"\nTarget: {target} - Methodology: {methodology}")
        
        content_weeks = ast.literal_eval(olap_sync_item['content_weeks'])
        failed = 0
        for week_ending in content_weeks:
            
            # Itera le content week per aggiornare gli olap square
            # Fornisco project_id, data_week, filename per rintracciare il file UQ sorgente
            # Fornisco content_week per identificare lo square di destinazione
            if failed == 0:
                outcome = generate_olap_reports(project_id, week_ending, target, methodology)
                if not outcome:
                    failed +=1
                
            # OUTCOME IS POSITIVE FOR ALL CONTENT_WEEKS

        if not failed:
            #print("Olap Sync SUCCESS!")
            transformation_queue.mark_olap_synced(olap_sync_item_id)
        else:
            print("Olap Sync Failed!")


    print(f"[INFO] OLAP Sync Phase Ended ({total_olap_sync_items} items synced)")
    logger.info(f"OLAP Sync Phase Ended ({total_olap_sync_items} items synced)")   