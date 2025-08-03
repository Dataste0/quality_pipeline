import os
import pandas as pd
import re
import ast
import logging
import json
import pipeline_lib.pipeline_utils as pu
from pipeline_lib.queues import SnapshotManager, TransformationQueueManager
import pipeline_lib.config as cfg

RAW_DATA_ROOT = cfg.RAWDATA_ROOT_PATH

SNAPSHOT_QUEUE_FILE = cfg.SNAPSHOT_FILE_PATH
TRANSFORMATION_QUEUE_FILE = cfg.QUEUE_TRANSFORMATION_FILE_PATH

PROJECT_MASTERFILE = cfg.PROJECT_INFO_FILE_PATH


# --- Setup queues
snapshot_queue = SnapshotManager(SNAPSHOT_QUEUE_FILE)
transformation_queue = TransformationQueueManager(TRANSFORMATION_QUEUE_FILE)

# --- Setup logger
logger = logging.getLogger('pipeline.fetch')

# --- Setup Project List
project_list_df = pu.load_project_info(PROJECT_MASTERFILE, active_only=False)


# --- Scan weekly data folder
def scan_rawdata_week_folder(
    project_id,
    project_name,
    project_is_active,
    project_config_list,
    data_week,
    raw_data_root,
    last_snapshot,
    create_missing
):

    logger.info(f"Scanning folder: {project_id} | {project_name} // {data_week}")
    print(f"Scanning folder: {project_id} | {project_name} // {data_week}")

    week_folder_info = pu.get_week_folder(data_week, project_id, raw_data_root)
    folder_path = week_folder_info["path"]
    folder_name = week_folder_info["name"]

    if not week_folder_info["exists"]:
        if create_missing:
            os.makedirs(folder_path, exist_ok=True)
            logger.debug(f"Weekly Folder created for {project_id} ({project_name}): {folder_name}")
        else:
            logger.warning(f"Weekly Folder missing and not created for {project_id} ({project_name}): skipping.")
            return None
    
    # Fast Hash of directory
    directory_hash = pu.hash_directory_fast(folder_path, project_is_active) # want to ensure hash changes if project switches inactive -> active
    #print(f"Directory Fast hash ready: {directory_hash} - Previous hash: {last_snapshot['folder_hash']}")

    if not last_snapshot.empty and last_snapshot["folder_hash"] == directory_hash:
        #print("Weelky folder hash match. No changes made.")
        logger.debug(f"Weekly Folder matches previous status. No changes made. {project_id} ({project_name}) {folder_name}")
        return {
            "project_id": project_id,
            "project_name": project_name,
            "data_week": data_week,
            "folder_hash": directory_hash,
            "has_any_data": last_snapshot['has_any_data'],
            "has_weekly_data": last_snapshot['has_weekly_data'],
            "file_number": last_snapshot['file_number'],
            "file_list": last_snapshot['file_list'],
            "valid_files_number": last_snapshot['valid_files_number'],
            "valid_files_list": last_snapshot['valid_files_list']
        }
    
    #print(f"Weekly Folder doesnt match previous hash. Checking folder content... {project_id} ({project_name}) {folder_name}")
    logger.info(f"Weekly Folder doesnt match previous hash. Checking folder content... {project_id} ({project_name}) {folder_name}")
    try:
        # Consider only non-empty CSV/Excel files with at least one data row
        allowed_exts = ('.csv', '.xls', '.xlsx')
        files_available = [
            f for f in os.listdir(folder_path)
            if f.lower().endswith(allowed_exts)
            and os.path.getsize(os.path.join(folder_path, f)) > 0
            and pu.has_at_least_one_data_row(os.path.join(folder_path, f))
        ]

        if not files_available:
            logger.debug(f"Empty folder: {folder_path}")


        file_existing_list = []
        
        for f in sorted(files_available):
            
            full_path = os.path.join(folder_path, f)
            file_hash = None
            regex_matched = False
            format_ok = False

            for item in project_config_list:
                item_regex = item['files_filter_regex']
                item_pattern = item['files_filter_pattern']
                if item_regex is None:
                    continue
                
                if item_regex.match(f):
                    regex_matched = True
                    dataset_fingerprint = item.get("dataset_fingerprint")
                    header_computed = pu.hash_header(full_path)
                    format_ok = header_computed == dataset_fingerprint

                    file_hash = pu.hash_file(full_path)
                    # trovato il primo match: esci dal loop degli item
                    break
            
            logger.debug(f"File {f} - Regex match: {regex_matched} - Format OK: {format_ok}")
                         
            file_existing_list.append({
                "filename": f,
                "hash": file_hash,
                "naming_filter" : "match" if regex_matched else "no_match",
                "dataset_fingerprint": "match" if format_ok else "no_match"
            })


        matching_files = [
            info for info in file_existing_list
            if info["naming_filter"] == "match" and info["dataset_fingerprint"] == "match"
        ]

        if file_existing_list and not matching_files:
            logger.warning(
                f"No valid matches in {project_id} ({project_name}) {folder_name} | "
                f"Files: {[f['filename'] for f in file_existing_list]}"
            )

        logger.debug(f"Weekly folder scan complete for {project_id} ({project_name}).")

        return {
            "project_id": project_id,
            "project_name": project_name,
            "data_week": data_week,
            "folder_hash": directory_hash,
            "has_any_data": bool(file_existing_list),
            "has_weekly_data": bool(matching_files),
            "file_number": len(file_existing_list),
            "file_list": file_existing_list,
            "valid_files_number": len(matching_files),
            "valid_files_list": matching_files
        }

    except Exception as e:
        print(f"Access denied to {project_id} ({project_name}) {folder_name}: {e}")
        logger.error(f"Access denied to {project_id} ({project_name}) {folder_name}: {e}")
        return None



def scan_rawdata_project_folder(
    project_id,
    project_name,
    project_is_active,
    project_folder_name,
    project_start_date,
    project_end_date,
    project_metadata,
    raw_data_root,
    last_snapshot,
    create_missing
):

    logger.debug(
        f"Scanning project folder: {project_name} | create_missing={create_missing}, hash={hash}"
    )

    metadata_dict = json.loads(project_metadata)
    project_config_list = metadata_dict.get("project_config", [])
    scan_log = []
    

    for item in project_config_list:
        file_pattern_dict = item.get("files_filter", {})
        file_pattern_flat = pu.extract_file_pattern(file_pattern_dict)

        # Validate file pattern
        if not isinstance(file_pattern_flat, str) or not file_pattern_flat.strip():
            logger.warning(f"No file_pattern defined for {project_id} ({project_name}). Using wildcard ALL files.")
            file_pattern_flat = ".*"

        try:
            regex = re.compile(file_pattern_flat)
        except re.error:
            logger.warning(f"Invalid regex for {project_id} ({project_name})")
            return None
        
        item['files_filter_regex'] = regex
        item['files_filter_pattern'] = file_pattern_flat
        

    project_folder_info = pu.get_project_folder(project_id, raw_data_root)

    if not project_folder_info["exists"]:
        logger.debug(f"Project folder not found for {project_id} ({project_name})")
        if create_missing:
            project_folder_path = os.path.join(raw_data_root, project_folder_name)
            os.makedirs(project_folder_path, exist_ok=True)
            logger.info(f"Created project folder for {project_id} ({project_name}): {project_folder_name}")
        else:
            logger.warning(f"Project folder missing and not created for {project_id} ({project_name}). Skipping.")
            return []
    else:
        project_folder_path = project_folder_info["path"]

    # Generate list of weekly end dates between start and end
    date_list = pu.generate_we_dates(project_start_date, project_end_date)
    logger.debug(f"Generated weekly dates: {date_list}")

    for we_date in date_list:
        we_date = we_date.strftime("%Y-%m-%d")
        
        filtered_snapshot = last_snapshot[
            (last_snapshot['data_week'] == we_date) &
            (last_snapshot['project_id'] == project_id)
        ]
        if not filtered_snapshot.empty:
            snapshot_row = filtered_snapshot.iloc[0].copy()
        else:
            snapshot_row = pd.Series(dtype='object')

        result = scan_rawdata_week_folder(
            project_id=project_id,
            project_name=project_name,
            project_is_active=project_is_active,
            project_config_list=project_config_list,
            data_week=we_date,
            raw_data_root=raw_data_root,
            last_snapshot=snapshot_row,
            create_missing=create_missing
        )
        if result:
            scan_log.append(result)

    logger.debug(f"Completed scan for project {project_id} ({project_name}).")
    return scan_log



# --- Scans all projects in Project List (Generates: current Snapshot)
def scan_rawdata(project_df, raw_data_root, last_snapshot, create_missing):
    logger.debug("Starting scan of rawdata folders.")
    project_df.to_csv('miotest.csv')

    if not os.path.exists(raw_data_root):
        logger.error(f"Raw data root folder not found: {raw_data_root}. Aborting.")
        return None

    scan_log = []

    for _, row in project_df.iterrows():
        result = scan_rawdata_project_folder(
            project_id=row["project_id"],
            project_name=row["project_name"],
            project_is_active=row["project_is_active"],
            project_folder_name=row["raw_folder_name"],
            project_start_date=row["project_start_date"],
            project_end_date=row["project_end_date"],
            project_metadata=row['project_metadata'],
            raw_data_root=raw_data_root,
            last_snapshot=last_snapshot,
            create_missing=create_missing
        )

        if result:
            scan_log.extend(result)
        else:
            logger.warning(f"No scan result for project {row['project_id']} ({row['project_name']})")

    logger.debug("Rawdata folder scan complete.")
    return pd.DataFrame(scan_log)






######## Create snapshot ########

def generate_rawdata_snapshot():
    print(f"[INFO] Creating RawData Snapshot")
    logger.info(f"Creating rawdata snapshot")

    # Filter projects with track_data enabled
    project_df = project_list_df[project_list_df["track_data"] == True]

    # Get last snapshot for directory hash comparison
    last_snapshot_id = snapshot_queue.get_last_snapshot_no()
    last_snaphot = snapshot_queue.get_snapshot(last_snapshot_id)

    # Scans and populates the snapshot dataframe
    snapshot_df = scan_rawdata(project_df, RAW_DATA_ROOT, last_snaphot, create_missing=True)
    
    # Append to queue
    snapshot_queue.add_snapshot(snapshot_df)

    print(f"[INFO] RawData snapshot created")
    logger.info(f"Rawdata snapshot created")



######## Compare snapshots ########

def compare_rawdata_snapshots():
    print(f"[INFO] Comparing Snapshots")
    logger.info(f"Comparing Snapshots")

    current_snapshot_id = snapshot_queue.get_last_snapshot_no()
    previous_snapshot_id = snapshot_queue.get_previous_snapshot_no()

    if current_snapshot_id is None:
        # Snapshot doesnt exist, Abort
        logger.warning(f"Cannot compare snapshots: snapshot not found.")
        print(f"[WARNING] Cannot compare snapshots: snapshot not found.")
        return None
    

    current_df = snapshot_queue.get_snapshot(current_snapshot_id)
    # We are interested only in weeks with data
    current_df = current_df[current_df["has_weekly_data"] == True].copy()
    
    # Keep only relevant columns
    relevant_columns = [
        "project_id",
        "project_name",
        "data_week",
        "folder_hash",
        "has_weekly_data",
        "valid_files_list"
    ]
    current_df = current_df[relevant_columns].copy()
    
    if previous_snapshot_id is None:
        logger.warning(f"Previous rawdata log missing or empty. Returning all available weeks with data, for processing.")
        print("[WARNING] Previous rawdata log missing or empty. Returning all available weeks with data, for processing.")
        previous_df = pd.DataFrame(columns=current_df.columns)
    else:
        previous_df = snapshot_queue.get_snapshot(previous_snapshot_id)

    # Safe casting
    current_df["data_week"] = pd.to_datetime(current_df["data_week"])
    previous_df["data_week"] = pd.to_datetime(previous_df["data_week"])
    
    current_df["project_id"] = current_df["project_id"].astype(str).str.strip()
    previous_df["project_id"] = previous_df["project_id"].astype(str).str.strip()

    # Merge
    merged = pd.merge(
        previous_df,
        current_df,
        on=["project_id", "data_week"],
        how="right",
        suffixes=("_prev", "_curr")
    )

    # Safe casting
    merged["has_weekly_data_prev"] = (
        merged["has_weekly_data_prev"]
        .astype("boolean")
        .fillna(False)
        .astype(bool)
    )

    merged["has_weekly_data_curr"] = (
        merged["has_weekly_data_curr"]
        .astype("boolean")
        .fillna(False)
        .astype(bool)
    )

    # Find rows where weekly data availability has changed
    differing_rows = merged[(
        merged["folder_hash_prev"] != merged["folder_hash_curr"]
    )].copy()


    enqueued_items = []

    for _, row in differing_rows.iterrows():
        items = pu.compare_files_list(row["valid_files_list_prev"], row["valid_files_list_curr"])
        for item in items:
            enqueued_items.append({
                "snapshot_id": current_snapshot_id,
                "project_id": row["project_id"],
                "project_name": row["project_name_curr"],
                "data_week": row["data_week"].strftime("%Y-%m-%d"),
                "filename": item["filename"]
            })

    # Format: result [snapshot_id, project_id, project_name, data_week, filename]
    
    # Store to Transformation Queue
    i = 0
    for item in enqueued_items:
        print(f"[{i+1}/{len(enqueued_items)}] Enqueuing {item['filename']} | {item['project_id']} ({item['project_name']}) // {item['data_week']}")
        logger.info(f"[{i+1}/{len(enqueued_items)}] Enqueuing {item['filename']} | {item['project_id']} ({item['project_name']}) // {item['data_week']}")
        transformation_queue.push(item)
        i += 1

    #transformation_queue.append_df(enqueued_items)

    print(f"[INFO] Comparing Snapshots: Done. Enqueued {len(enqueued_items)} new files.")
    logger.info(f"Comparing Snapshots: Done. Enqueued {len(enqueued_items)} new files.")

