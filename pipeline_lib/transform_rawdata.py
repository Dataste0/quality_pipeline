import os
import traceback
import json
import pandas as pd
import logging
import pipeline_lib.pipeline_utils as pu
import pipeline_lib.config as cfg
from pipeline_lib.project_transformers.transformer_utils import process_dataframe
from pipeline_lib.queues import TransformationQueueManager

CSV_BASE_DIR = os.path.join(cfg.DATA_TRANSFORMED_DIR_PATH, cfg.CSV_DIR)
PARQUET_BASE_DIR = os.path.join(cfg.DATA_TRANSFORMED_DIR_PATH, cfg.PARQUET_DIR)

RAWDATA_BASE_DIR = cfg.RAWDATA_ROOT_PATH

TRANSFORMATION_QUEUE_FILE = cfg.QUEUE_TRANSFORMATION_FILE_PATH

PROJECT_MASTERFILE = cfg.PROJECT_INFO_FILE_PATH


# --- Setup queues
transformation_queue = TransformationQueueManager(TRANSFORMATION_QUEUE_FILE)

# --- Setup logger
logger = logging.getLogger('pipeline.transform')

# --- Setup Project List
project_list_df = pu.load_project_info(PROJECT_MASTERFILE, active_only=False)



# --- Process file by selecting the proper transformer and saves it to an output path as transformed
def process_file(raw_file_path, project_id, metadata_str, data_week):
    logger.debug(f"Processing file path: {raw_file_path}")
    #print(f"Processing file path: {raw_file_path}")
    
    # Get filename from path
    raw_filename = os.path.basename(raw_file_path)
    
    try:
        # Load File as Dataframe
        df = pu.load_df_from_filepath(raw_file_path)

        if df.empty:
            logger.warning(f"Empty Rawfile: {raw_file_path}")
            #print(f"[WARNING] Empty Rawfile: {raw_file_path}")
            return False, {"transform_error": "empty_source_file"}, None
        
        metadata = json.loads(metadata_str)

        metadata_str_clean = metadata_str.replace('\n', ' ').replace('\r', ' ').replace('\t', ' ')
        logger.debug(f"Processing file: {raw_file_path} - Json: {metadata_str_clean}")
        #print(f"Processing file: {raw_file_path} - Json: {metadata_str} - Output: {output_path}")

        if metadata.get("use_reporting_data") is True:
            reporting_week = data_week
            metadata["reporting_week"] = data_week
            logger.debug(f"'use_reporting_data' is True – setting 'reporting_week' to {reporting_week}")

        df_transformed, transformed_info = process_dataframe(df, metadata)

        if df_transformed.empty:
            logger.warning(f"Empty Transformed Dataframe: {raw_file_path}")
            #print(f"[WARNING] Empty Transformed Dataframe: {raw_file_path}")
            return False, {"transform_error": "empty_output_file"}|transformed_info, None
        
        #print(f"\nTransformed Info: {transformed_info}")

        logger.debug(f"Done processing dataframe.")

        #print(f"Processing dataframe... DONE")

        if 'submission_date' not in df_transformed.columns:
            return False, {"transform_error": "submission_date_column_missing"}|transformed_info, None
        
        content_weeks = pu.get_content_weeks(df_transformed['submission_date'])

        # Generate CSV output paths
        name_label = pu.clean_filename(raw_filename)
        data_week = pd.to_datetime(data_week, errors="coerce").strftime("%Y-%m-%d")
        csv_output_name = f"{project_id}_{data_week}_{name_label}_UQ.csv"
        csv_output_folder = os.path.join(CSV_BASE_DIR, "Transformed_" + project_id, data_week)
        os.makedirs(csv_output_folder, exist_ok=True)
        csv_output_path = os.path.join(csv_output_folder, csv_output_name)
        
        # Save to CSV output path
        pu.save_df_to_filepath(df_transformed, csv_output_path)
        logger.debug(f"Transformed and saved: {raw_filename} → {csv_output_path}")
        #print(f"Transformed and saved: {filename} → {output_path}")

        # Adapt to Parquet
        df_transformed['content_week'] = df_transformed['submission_date'].apply(pu.get_friday_of_week)

        # Generate PARQUET output paths
        parquet_output_folder = os.path.join(PARQUET_BASE_DIR, "Parq_" + project_id, data_week)
        os.makedirs(parquet_output_folder, exist_ok=True)

        parquet_filenames = []
        for content_week in content_weeks:
            content_week = pd.to_datetime(content_week, errors="coerce")
            cw_str = content_week.strftime("%Y%m%d")
            parquet_output_name = f"{project_id}_{data_week}_{name_label}_C{cw_str}.parquet"
            parquet_filenames.append(parquet_output_name)
            df_cw = df_transformed[df_transformed['content_week'] == content_week]
            df_cw.to_parquet(os.path.join(parquet_output_folder, parquet_output_name), index=False)
        
        param_dict = {
            "output_filename": csv_output_name,
            "content_weeks": content_weeks,
            "parquet_filenames": parquet_filenames
        }
        return True, transformed_info, param_dict

    except Exception as e:
        logger.error(f"Error transforming {raw_file_path}: {e} - Traceback: {traceback.format_exc()}")
        print(f"Error transforming {raw_file_path}: {e} - Traceback: {traceback.format_exc()}")
        #print(traceback.format_exc())
        return False, {"transform_error": f"{e}"}, None
    

# Process enqueued item to trasform

def process_item(row):
    project_id = row["project_id"]
    project_name = row["project_name"]
    data_week = pd.to_datetime(row["data_week"], errors="coerce").strftime("%Y-%m-%d")
    raw_filename = row["filename"]
    metadata = row.get("project_metadata")

    # Check if metadata is provided
    if not metadata or not isinstance(metadata, str):
        logger.error(f"Invalid metadata provided for file: {raw_file_path}")
        print(f"[ERROR] Invalid metadata provided for file: {raw_file_path}")
        return False, {"transform_error": "no_metadata"}

    # Acquire file path
    raw_file_folder = pu.get_week_folder(data_week, project_id, RAWDATA_BASE_DIR)["path"]
    #logger.debug(f"Filename type: {type(filename)} - value: {filename}")
    raw_file_path = os.path.join(raw_file_folder, raw_filename)

    logger.debug(f"Processing file: {raw_file_path} for project: {project_name} - Week: {data_week}")
    #print(f"Processing file: {raw_file_path} for project: {project_name} - Week: {data_week}")

    if not os.path.exists(raw_file_path):
        logger.error(f"Raw File Path does not exist: {raw_file_path}")
        print(f"Raw File Path does not exist: {raw_file_path}")
        return False, {"transform_error": "source_file_doesnt_exist"}

    # Process the file
    result, transform_info, param_dict = process_file(raw_file_path, project_id, metadata, data_week)
    
    return result, transform_info, param_dict


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
        columns_to_merge = ["project_id", "project_metadata"]
        enriched_df = enqueued_df.merge(project_list_df[columns_to_merge], on="project_id", how="left")
        logger.debug(f"Columns in enqueued_item after merge: {enriched_df.columns.tolist()}")

        row = enriched_df.iloc[0]
        raw_filename = row['filename']
        result, transform_info, param_dict = process_item(row)

        # Validate processing output
        if result:
            transform_result = "transformed"
            logger.info(f"Processed and saved: {param_dict['output_filename']} - WEEKENDINGS: {param_dict['content_weeks']}")
        else:
            transform_result = "failed"
            print(f"Transformation failed: {transform_info['transform_error']}")
            logger.error(f"Failed to process: {raw_filename}")
        
        # Enqueue
        transformation_queue.complete_transform(enqueued_item_id, transform_result, {
            "output_filename": param_dict['output_filename'] if result else "",
            "content_weeks": param_dict['content_weeks'] if result else "",
            "parquet_filenames": param_dict['parquet_filenames'] if result else "",
            "transform_info": json.dumps(transform_info)
        })

        # END PROCESSING ENQUEUED ITEM

    #print(f"[INFO] Transformation complete")
    logger.info(f"Transformation complete")
