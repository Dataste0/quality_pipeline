import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import pipeline_lib.config as cfg
import pipeline_lib.pipeline_utils as pu
from pipeline_lib.project_transformers.transformer_utils import process_dataframe
from pipeline_lib.queues import TransformationQueueManager, SnapshotManager

SNAPSHOT_QUEUE_FILE = cfg.SNAPSHOT_FILE_PATH
TRANSFORMATION_QUEUE_FILE = cfg.QUEUE_TRANSFORMATION_FILE_PATH

PROJECT_MASTERFILE = cfg.PROJECT_INFO_FILE_PATH

# --- Setup queues
snapshot_queue = SnapshotManager(SNAPSHOT_QUEUE_FILE)
transformation_queue = TransformationQueueManager(TRANSFORMATION_QUEUE_FILE)

# --- Setup Project List
project_list_df = pu.load_project_info(PROJECT_MASTERFILE, active_only=False)


def enqueue_project(project_id, data_week=None):
    # Uses available data weeks from last snapshot
    project_info = project_list_df.loc[project_list_df["project_id"] == project_id]
    project_name = project_info["project_name"]

    current_snapshot_id = snapshot_queue.get_last_snapshot_no()
    if current_snapshot_id is None:
        # Snapshot doesnt exist, Abort
        print(f"[WARNING] Cannot compare snapshots: snapshot not found.")
        return None
    
    snapshot_df = snapshot_queue.get_snapshot(current_snapshot_id)
    project_data_df = snapshot_df[snapshot_df["project_id"] == project_id]

    if data_week:
        project_data_df = project_data_df[project_data_df["data_week"] == data_week]


    enqueued_items = []

    for _, row in project_data_df.iterrows():
        items = row["valid_files_list"]
        for item in items:
            enqueued_items.append({
                "snapshot_id": -1,
                "project_id": project_id,
                "project_name": project_name,
                "data_week": row["data_week"].strftime("%Y-%m-%d"),
                "filename": item["filename"]
            })

    # Format: result [snapshot_id, project_id, project_name, data_week, filename]
    
    # Store to Transformation Queue
    i = 0
    for item in enqueued_items:
        print(f"[{i+1}/{len(enqueued_items)}] Enqueuing {item['filename']} | {item['project_id']} ({item['project_name']}) // {item['data_week']}")
        transformation_queue.push(item)
        i += 1
