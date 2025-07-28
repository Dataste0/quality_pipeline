import os

# RawData Directory Tree
"""
RAWDATA_ROOT_PATH
└── <projectid>_<projectfullname>
    ├── WE <yyyy.mm.dd>
    ├── WE <yyyy.mm.dd>
    ...
    └── WE <yyyy.mm.dd>
"""
RAWDATA_ROOT_PATH = "/mnt/c/rawdata"


# Pipeline Directory Tree
"""
PIPELINE_ROOT_PATH
├── Data_Process
│   ├── queues
│   └── logs
|
├── Data_Transformed
|   ├── CSV
│   |   ├── Transformed_<projectid-1>
|   |   |   └── <yyyy-mm-dd>
|   |   |       └── <projectid-1>_<yyyy-mm-dd>_<string>_UQ.csv
|   |   └── ...
|   └── Parquet
│       ├── Parq_<projectid-1>
|       |   └── <yyyy-mm-dd>
|       |       ├── <projectid-1>_<yyyy-mm-dd>_<string>_C<yyyymmdd>.parquet
|       |       └── <projectid-1>_<yyyy-mm-dd>_<string>_C<yyyymmdd>.parquet
|       └── ...
|
└── OLAP_Export
    ├── Project_<projectid-1>
    │   ├── <yyyy-mm-dd>
    │   ...
    │   └── <yyyy-mm-dd>
    ...
    ├── Project_<projectid-n>
    ├── project_masterfile.xlsx (*)
    └── snapshot_rawdata_folders.csv (*)
"""
PIPELINE_ROOT_PATH = "/mnt/c/dashboard"

DATA_PROCESS_DIR = "Data_Process"
DATA_QUEUES_DIR = "queues"
DATA_LOG_DIR = "logs"
DATA_LOG_DIR_PATH = os.path.join(PIPELINE_ROOT_PATH, DATA_PROCESS_DIR, DATA_LOG_DIR)

DATA_TRANSFORMED_DIR = "Data_Transformed"
DATA_TRANSFORMED_DIR_PATH = os.path.join(PIPELINE_ROOT_PATH, DATA_TRANSFORMED_DIR)
CSV_DIR = "CSV"
PARQUET_DIR = "Parquet"
DATA_PARQUET_DIR_PATH = os.path.join(DATA_TRANSFORMED_DIR_PATH, PARQUET_DIR)

OLAP_DIR = "OLAP_Export"
OLAP_EXPORT_DIR_PATH = os.path.join(PIPELINE_ROOT_PATH, OLAP_DIR)


# Project Masterfile

PROJECT_INFO_FILE = "project_masterfile.xlsx"
PROJECT_INFO_FILE_PATH = os.path.join(PIPELINE_ROOT_PATH, OLAP_DIR, PROJECT_INFO_FILE)


# Queues and Buffers

SNAPSHOT_FILE = "snapshot_rawdata_folders.csv"
SNAPSHOT_FILE_PATH = os.path.join(PIPELINE_ROOT_PATH, OLAP_DIR, SNAPSHOT_FILE)

QUEUE_TRANSFORMATION_FILE = "transformation_queue.csv"
QUEUE_TRANSFORMATION_FILE_PATH = os.path.join(PIPELINE_ROOT_PATH, DATA_PROCESS_DIR, DATA_QUEUES_DIR, QUEUE_TRANSFORMATION_FILE)

