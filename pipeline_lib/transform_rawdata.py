import os
import traceback
import json
import pandas as pd

import pipeline_lib.pipeline_utils as pu
import pipeline_lib.config as cfg
from pipeline_lib.project_transformers.dispatcher import process_dataframe
from pipeline_lib.queues import TransformationQueueManager

RAWDATA_BASE_DIR = cfg.RAWDATA_ROOT_PATH
PARQUET_BASE_DIR = os.path.join(cfg.DATA_TRANSFORMED_DIR_PATH)

# --- Setup queues
TRANSFORMATION_QUEUE_FILE = cfg.QUEUE_TRANSFORMATION_FILE_PATH
transformation_queue = TransformationQueueManager(TRANSFORMATION_QUEUE_FILE)

# --- Setup Project List
PROJECT_MASTERFILE = cfg.PROJECT_INFO_FILE_PATH
project_list_df = pu.load_project_info(PROJECT_MASTERFILE, active_only=False)

# --- Logger
import logging
logger = logging.getLogger(__name__)


# --- Process file by selecting the proper transformer and saves it to an output path as transformed
def process_file(raw_file_path, project_metadata, data_week):
    logger.debug(f"Processing file path: {raw_file_path}")

    project_id = project_metadata.get("project_id", None)

    # Get filename from path
    raw_filename = os.path.basename(raw_file_path)

    process_file_dict = {}
    
    try:
        df = pu.load_df_from_filepath(raw_file_path)

        if df.empty:
            logger.warning(f"Empty Rawfile: {raw_file_path}")
            process_file_dict["transform_info"] = {"transform_error": "empty_source_file"}
            return False, process_file_dict

        logger.debug(f"Processing file: {raw_file_path}")
        #print(f"Processing file: {raw_file_path} - Json: {metadata_str} - Output: {output_path}")

        # Determine module to use
        project_config = project_metadata.get("project_config", {})
        
        project_base = project_metadata.get("project_base", None)
        base_code = project_base.upper()[0] if isinstance(project_base, str) and project_base in ["halo", "multi", "audit"] else None
        if not project_base or not base_code:
            logger.error(f"Project base not defined for project {project_id}")
            process_file_dict["transform_info"] = {"transform_error": "project_base_not_defined"}
            return False, process_file_dict
        
        dataset_type = project_config.get("dataset_type", None)
        module = project_config.get("module",None)
        if not dataset_type:
            logger.error(f"Dataset type missing in declaration")
            process_file_dict["transform_info"] = {"transform_error": "dataset_type_missing_in_declaration"}
            return False, process_file_dict
        if not module:
            logger.error(f"Module missing in declaration")
            process_file_dict["transform_info"] = {"transform_error": "module_missing_in_declaration"}
            return False, process_file_dict
        

        project_config['module_config']['reporting_week'] = pd.to_datetime(data_week, errors="coerce").strftime("%Y-%m-%d") # Add reporting week for dataset with no job date

        # Check for roster file
        dir_path = os.path.dirname(raw_file_path)
        roster_file_path = os.path.join(dir_path, "roster.xlsx")
        if os.path.exists(roster_file_path):
            roster_dict = pu.load_roster_list(roster_file_path)
            project_config['module_config']['roster_list'] = roster_dict
            logger.debug(f"Roster file found and loaded: {roster_file_path}")


        ### PROCESS DATAFRAME ###
        df_transformed, transformed_dict = process_dataframe(df, project_metadata)
        process_file_dict["transform_info"] = {"project_id": project_metadata["project_id"], "project_name": project_metadata["project_name"]} | transformed_dict

        if df_transformed.empty:
            logger.warning(f"Empty Transformed Dataframe: {raw_file_path}")
            process_file_dict["transform_info"] = {"transform_error": "empty_output_file" } | transformed_dict
            return False, process_file_dict
        
        logger.debug(f"Done processing dataframe.")
        #print(f"\nTransformer DICT READ::{transformed_dict}")

        data_week = pd.to_datetime(data_week, errors="coerce")
        df_transformed.insert(0, "reporting_week", data_week)
        df_transformed.insert(0, "project_id", project_id)

        name_label = pu.clean_filename(raw_filename)
        data_week_str = data_week.strftime("%Y-%m-%d")
        

        # Generate PARQUET output paths
        parquet_output_folder = os.path.join(PARQUET_BASE_DIR, project_id, data_week_str)
        os.makedirs(parquet_output_folder, exist_ok=True)

        parquet_filenames = []
        content_weeks_list = []
        unique_content_weeks = df_transformed['content_week'].unique().tolist()
        if len(unique_content_weeks) == 0:
            process_file_dict["transform_info"] = {"transform_error": "content_weeks_list_empty" } | transformed_dict
            return False, process_file_dict

        #print(f"\nContent weeks list: {unique_content_weeks}")
        for content_week in unique_content_weeks:
            content_weeks_list.append(content_week)
            cw_str = pd.to_datetime(content_week, errors="coerce").strftime("%Y%m%d")
            
            parquet_output_name = f"{project_id}_{data_week_str}_{name_label}_{base_code}_{cw_str}.parquet"
            parquet_filenames.append(parquet_output_name)
            
            mask = df_transformed["content_week"] == content_week
            df_cw = df_transformed.loc[mask]
            df_cw.to_parquet(os.path.join(parquet_output_folder, parquet_output_name), index=False)
            
        process_file_dict["output_filenames"]   = parquet_filenames
        process_file_dict["content_weeks"]      = content_weeks_list
        return True, process_file_dict

    except Exception as e:
        logger.error(f"Error transforming {raw_file_path}: {e} - Traceback: {traceback.format_exc()}")
        print(f"Error transforming {raw_file_path}: {e} - Traceback: {traceback.format_exc()}")
        #print(traceback.format_exc())
        return False, {"transform_error": f"{e}"}, None
    

# Process enqueued item to trasform

def process_item(item):
    project_id = item["project_id"]
    data_week = pd.to_datetime(item["data_week"], errors="coerce")
    raw_filename = item["filename"]

    project_metadata = pu.get_project_metadata(project_id, project_list_df)
    project_name = project_metadata["project_name"]

    process_item_dict = {}
    # Check if metadata is provided
    if not project_metadata or not isinstance(project_metadata, dict):
        logger.error(f"Invalid metadata provided for file: {raw_file_path}")
        print(f"[ERROR] Invalid metadata provided for file: {raw_file_path}")
        process_item_dict["transform_info"] = {"transform_error": "no_metadata_provided"}
        return False, process_item_dict

    # Acquire file path
    raw_file_folder = pu.get_week_folder(data_week, project_id, RAWDATA_BASE_DIR)["path"]
    raw_file_path = os.path.join(raw_file_folder, raw_filename)

    logger.debug(f"Processing file: {raw_file_path} for project: {project_name} - Data Week: {data_week}")

    if not os.path.exists(raw_file_path):
        logger.error(f"Raw File Path does not exist: {raw_file_path}")
        print(f"Raw File Path does not exist: {raw_file_path}")
        process_item_dict["transform_info"] = {"transform_error": "source_file_doesnt_exist"}
        return False, process_item_dict

    # Process the file
    result, process_file_dict = process_file(raw_file_path, project_metadata, data_week)

    process_item_dict = {
        "transform_info"    : process_file_dict.get("transform_info"),
        "content_weeks"     : process_file_dict.get("content_weeks", ""),
        "output_filenames"  : process_file_dict.get("output_filenames", ""),
    }

    return result, process_item_dict


def transform_enqueued_items():
    total_enqueued = transformation_queue.count(status="enqueued")
    print(f"[INFO] Transforming enqueued files ({total_enqueued} files)")
    logger.info(f"Transforming enqueued files ({total_enqueued} files)")

    for i in range(total_enqueued):
        enqueued_item = transformation_queue.pop()
        if not enqueued_item:
            logger.warning(f"Queue empty before expected!")
            break
        enqueued_item_id = enqueued_item['item_id']
        logger.debug(f"[{i+1}/{total_enqueued}] Processing Item ID {enqueued_item_id}: {enqueued_item}")
        
        print(f"[{i+1}/{total_enqueued}] Processing ItemID {enqueued_item_id} | {enqueued_item['project_id']} | {enqueued_item['project_name']} | WE {enqueued_item['data_week']}")
        
        # START PROCESSING ENQUEUED ITEM
        result, process_dict = process_item(enqueued_item)

        output_filenames    = process_dict.get("output_filenames", "")
        content_weeks       = process_dict.get("content_weeks", "")

        transform_info      = process_dict.get("transform_info", {})
        transform_error     = transform_info.get("transform_error", {})
        

        # Validate processing output
        if result:
            transform_result = "transformed"
            logger.info(f"Processed and saved: {output_filenames} - Content Weeks: {content_weeks}")
        else:
            transform_result = "failed"
            print(f"Transformation failed: {transform_error}")
            logger.error(f"Failed to process: {enqueued_item['filename']}")
        
        # Enqueue
        transformation_queue.complete_transform(enqueued_item_id, transform_result, {
            "output_filenames": json.dumps(output_filenames),
            "content_weeks": json.dumps(content_weeks),
            "transform_info": json.dumps(transform_info)
        })

        # END PROCESSING ENQUEUED ITEM

    #print(f"[INFO] Transformation complete")
    logger.info(f"Transformation complete")
