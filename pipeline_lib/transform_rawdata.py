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
def process_file(raw_file_path, project_id, metadata, data_week):
    logger.debug(f"Processing file path: {raw_file_path}")

    #print(f"DEBUG Processing. Metadata: {metadata}")
    
    # Get filename from path
    raw_filename = os.path.basename(raw_file_path)

    process_file_dict = {}
    
    try:
        df = pu.load_df_from_filepath(raw_file_path)

        if df.empty:
            logger.warning(f"Empty Rawfile: {raw_file_path}")
            process_file_dict["transform_info"] = {"transform_error": "empty_source_file"}
            return False, process_file_dict

        logger.debug(f"Processing file: {raw_file_path} - Json: {json.dumps(metadata)}")
        #print(f"Processing file: {raw_file_path} - Json: {metadata_str} - Output: {output_path}")

        # Determine module to use
        file_fingerprint = pu.hash_header(raw_file_path)
        
        module_found = False
        modules_available = metadata.get("project_config", [])
        #print(f"\nTRANSFORM RAWDATA - DEBUG metadata: {metadata}")
        for module_info in modules_available:
            dataset_fingerprint = module_info.get("dataset_fingerprint", None)
            if dataset_fingerprint == file_fingerprint:
                module_found = True
                #print(f"\nTRANSFORM RAWDATA - DEBUG module info: {module_info}")
                module_info['project_id'] = metadata['project_id'] # Add project id to module info for dispatching ADHOC modules
                module_info['reporting_week'] = pd.to_datetime(data_week, errors="coerce").strftime("%Y-%m-%d") # Add reporting week for dataset with no job date
                df_transformed, transformed_dict = process_dataframe(df, module_info)
                process_file_dict["transform_info"] = {"project_id": metadata["project_id"], "project_name": metadata["project_name"]} | transformed_dict

        if not module_found:
            logger.error(f"Unable to find valid project config - Dataset mismatch")
            process_file_dict["transform_info"] = {"transform_error": "no_dataset_fingerprint_match"}
            return False, process_file_dict

        if df_transformed.empty:
            logger.warning(f"Empty Transformed Dataframe: {raw_file_path}")
            #print(f"[WARNING] Empty Transformed Dataframe: {raw_file_path}")
            process_file_dict["transform_info"] = {"transform_error": "empty_output_file" } | transformed_dict
            return False, process_file_dict
        
        logger.debug(f"Done processing dataframe.")
        #print(f"\nTransformer DICT READ::{transformed_dict}")

        if "etl" in transformed_dict["etl"]:
            model_base = transformed_dict["etl"]["etl"]["base_info"]["model_base"]
        else:
            model_base = transformed_dict["etl"]["base_info"]["model_base"]
        
        base_code = None
        if isinstance(model_base, str) and model_base.startswith("BASE-") and len(model_base) > 5:
            base_code = model_base[5]
        else:
            process_file_dict["transform_info"] = {"transform_error": "model_base_undetermined" } | transformed_dict
            return False, process_file_dict

        data_week = pd.to_datetime(data_week, errors="coerce")
        df_transformed.insert(0, "reporting_week", data_week)
        df_transformed.insert(0, "project_id", project_id)

        name_label = pu.clean_filename(raw_filename)
        data_week_str = data_week.strftime("%Y-%m-%d")

        project_folder_name = metadata["raw_folder_name"]
        
        # Generate PARQUET output paths
        parquet_output_folder = os.path.join(PARQUET_BASE_DIR, project_id, data_week_str)
        os.makedirs(parquet_output_folder, exist_ok=True)

        parquet_filenames = []
        content_weeks_list = []
        unique_content_weeks = pd.to_datetime(df_transformed["content_week"], errors="coerce").dropna().unique()

        for content_week in unique_content_weeks:
            content_week_str = content_week.strftime("%Y-%m-%d")
            content_weeks_list.append(content_week_str)
            cw_str = content_week.strftime("%Y%m%d")
            
            parquet_output_name = f"{project_id}_{data_week_str}_{name_label}_{base_code}_{cw_str}.parquet"
            parquet_filenames.append(parquet_output_name)
            
            mask = pd.to_datetime(df_transformed["content_week"], errors="coerce") == content_week
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

def process_item(row):
    project_id = row["project_id"]
    project_name = row["project_name"]
    data_week = pd.to_datetime(row["data_week"], errors="coerce")
    raw_filename = row["filename"]
    metadata = json.loads(row['project_metadata'])
    metadata["raw_folder_name"] = row["raw_folder_name"]

    process_item_dict = {}

    # Check if metadata is provided
    if not metadata or not isinstance(metadata, dict):
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
    #result, transform_info = process_file(raw_file_path, project_id, metadata, data_week)
    result, process_file_dict = process_file(raw_file_path, project_id, metadata, data_week)

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
        enqueued_df = pd.DataFrame([enqueued_item])
        columns_to_merge = ["project_id", "project_metadata", "raw_folder_name"]
        enriched_df = enqueued_df.merge(project_list_df[columns_to_merge], on="project_id", how="left")
        logger.debug(f"Columns in enqueued_item after merge: {enriched_df.columns.tolist()}")

        row = enriched_df.iloc[0]
        raw_filename = row['filename']

        # result = True/False
        # process_dict = {
        #   "output_filenames": ""
        #   "content_weeks": ""
        #   "transform_info": {
        #       "transform_error": {}
        #       ...
        #    }
        # }
        result, process_dict = process_item(row)

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
            logger.error(f"Failed to process: {raw_filename}")
        
        # Enqueue
        transformation_queue.complete_transform(enqueued_item_id, transform_result, {
            "output_filenames": output_filenames,
            "content_weeks": content_weeks,
            "transform_info": json.dumps(transform_info)
        })

        # END PROCESSING ENQUEUED ITEM

    #print(f"[INFO] Transformation complete")
    logger.info(f"Transformation complete")
