import os
from pathlib import Path
from dotenv import load_dotenv

env_path = Path("folderpath.env.local")
load_dotenv(dotenv_path=env_path)

RAWDATA_ROOT_PATH = os.getenv("RAWDATA_ROOT_PATH", "C:\\rawdata")
PIPELINE_ROOT_PATH = os.getenv("PIPELINE_ROOT_PATH", "C:\\dashboard")
CQR_ROOT_PATH = os.getenv("CQR_ROOT_PATH", "C:\\cqr")

# RawData Directory Tree
"""
RAWDATA_ROOT_PATH
└── <projectid>_<projectfullname>
    ├── WE <yyyy.mm.dd>
    ├── WE <yyyy.mm.dd>
    ...
    └── WE <yyyy.mm.dd>
"""

# Pipeline Directory Tree
"""
PIPELINE_ROOT_PATH
├── Data_Process
│   ├── queues
│   └── logs
|
├── Data_Transformed
|   └── Parquet
│       ├── <projectid-1>_<project-name-1>
|       |   └── <yyyy-mm-dd>
|       |       ├── <projectid-1>_<yyyy-mm-dd>_<string>_C<yyyymmdd>.parquet
|       |       └── <projectid-1>_<yyyy-mm-dd>_<string>_C<yyyymmdd>.parquet
|       └── ...
|
└── OLAP_Export
    ├── <projectid-1>_<project-name-1>
    │   ├── <yyyy-mm-dd>
    │   ...
    │   └── <yyyy-mm-dd>
    ...
    ├── <projectid-n>_<project-name-n>
    ├── project_masterfile.xlsx (*)
    └── snapshot_rawdata_folders.csv (*)
"""

DATA_PROCESS_DIR = "Data_Process"
DATA_LOG_DIR_PATH = os.path.join(PIPELINE_ROOT_PATH, DATA_PROCESS_DIR)

DATA_TRANSFORMED_DIR = "Data_Transformed"
DATA_TRANSFORMED_DIR_PATH = os.path.join(PIPELINE_ROOT_PATH, DATA_TRANSFORMED_DIR)
DATA_PARQUET_DIR_PATH = os.path.join(DATA_TRANSFORMED_DIR_PATH)

UQV2_DIR = "UniversalQualityV2"
UQV2_DIR_PATH = os.path.join(PIPELINE_ROOT_PATH, UQV2_DIR)

OLAP_DIR = "OLAP_Export"
OLAP_EXPORT_DIR_PATH = os.path.join(PIPELINE_ROOT_PATH, OLAP_DIR, "Export")


# Project Masterfile

PROJECT_INFO_FILE = "project_masterfile.xlsx"
PROJECT_INFO_FILE_PATH = os.path.join(PIPELINE_ROOT_PATH, OLAP_DIR, PROJECT_INFO_FILE)


# Queues and Buffers

SNAPSHOT_FILE = "snapshot_rawdata_folders.csv"
SNAPSHOT_FILE_PATH = os.path.join(PIPELINE_ROOT_PATH, OLAP_DIR, SNAPSHOT_FILE)

QUEUE_TRANSFORMATION_FILE = "transformation_queue.csv"
QUEUE_TRANSFORMATION_FILE_PATH = os.path.join(PIPELINE_ROOT_PATH, DATA_PROCESS_DIR, QUEUE_TRANSFORMATION_FILE)


# PowerBI Refresh Webhook
PBI_REFRESH_WEBHOOK = os.getenv("PBI_REFRESH_WEBHOOK_URL")


# Dataset Header
DATASET_HEADER = {
    "HALO":         ["SRT Annotator ID", "Vendor Auditor ID", "SRT Job ID", "Time (PT)", "Vendor Tag"],

    "GALA":         ["annotator_id", "auditor_id", "task_id", "task_name", "audit_status", "rubric_answer", "QA_score", "original_submission_time"],

    "ADAP":         ["_unit_id", "_created_at", "_worker_id", "_tainted", "_channel"],

    "SPOTCHECK":    ["project_id", "queue", "job_id", "actor_id", "review_date", "label", "ground_truth", "is_spotcheck"],

    "UQD":          ["actor_id", "quality_actor_id", "job_id", "review_ds", "queue_name", "decision_data", "quality_decision_data", "extracted_label"],

    "CVS":          ["sample_ds", "entity_id", "rater_id", "routing_name", "rater_decision_data", "auditor_decision_data", "confusion_type", "config"],
    
    "UQD-LIKE":     ["actor_id", "decision_data", "decision_id", "job_final_derived_state", "job_id", "last_review_ds", "queue_name"],
    
    "HALO-LIKE":    ["Annotator ID", "Annotation Date And Time", "Annotation Job ID", "Annotation AHT s", "Audit Date And Time", "Is Job Successful?"],
    
    "MULTI-UNPIVOTED": ["job_id", "reviewer_id", "question", "answer"],
}


# Default Start Date for Data Processing (won't process data before this date)
START_DATE_DEFAULT = "2025-07-01"

# Universal Quality V2 Schema
UQ_V2_SCHEMA = [
    "project_id",
    "reporting_week",
    "content_week",
    "workflow",
    "job_date",
    "rater_id",
    "auditor_id",
    "job_id",
    "job_overall_correct",
    "job_manual_score",
    "label",
    "rater_response",
    "auditor_response",
    "is_correct",
    "is_label_binary",
    "confusion_type",
    "weight",
    "rubric_credit"
]