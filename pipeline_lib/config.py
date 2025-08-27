import os
from pathlib import Path
from dotenv import load_dotenv

env_path = Path("folderpath.env.local")
load_dotenv(dotenv_path=env_path)

RAWDATA_ROOT_PATH = os.getenv("RAWDATA_ROOT_PATH", "C:\\rawdata")
PIPELINE_ROOT_PATH = os.getenv("PIPELINE_ROOT_PATH", "C:\\pipeline")

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
