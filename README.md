
# quality_pipeline

A modular data pipeline for processing, transforming, and preparing raw data into a standardized quality format for PowerBI export.

---

## Overview

This repository provides a robust, script-driven pipeline for automating the transformation and export of raw data files for analytical processing. The pipeline is designed for incremental updates and extensibility across multiple project types. Historical backfill is not supported.

---

## Features

- **Automated raw data scanning and snapshotting**
- **Transformation and enrichment of raw data using project-specific logic**
- **Queue-based management of data processing stages**
- **OLAP export and Power BI refresh integration**
- **Configurable via environment and config files**
- **Extensible transformer modules for new project types**

---

## Directory Structure

- `main.py`  
  Entry point for the pipeline. Handles argument parsing and orchestrates the workflow.

- `requirements.txt`  
  Lists all Python dependencies required for the pipeline.

- `folderpath.env.local`  
  Environment variable file for local configuration (paths, secrets, etc).

- `pipeline_lib/`  
  Core library for the pipeline, containing:
  - `config.py` – Loads environment variables and defines all key paths and directory structures.
  - `logging_config.py` – Sets up rotating file logging for the pipeline.
  - `rawdata_fetch.py` – Scans raw data folders, generates snapshots, and manages the snapshot queue.
  - `transform_rawdata.py` – Transforms enqueued raw data using project-specific logic.
  - `olap_sync.py` – Handles OLAP export logic and report generation.
  - `powerbi.py` – Triggers Power BI dataset refresh via webhook.
  - `queues.py` – Implements CSV-based queue managers for tracking pipeline state.
  - `pipeline_utils.py` – Utility functions for loading project metadata, filtering, and more.
  - `baits_exception.py` – Specialized exception handling and data preparation for certain projects.
  - `project_transformers/` – Contains modular transformers for each project type:
    - `dispatcher.py` – Dispatches transformation to the correct module.
    - `mod_cvs.py`, `mod_halo.py`, `mod_uqd.py`, `mod_generic.py`, etc. – Project-specific transformation logic.
    - `transformer_utils.py` – Shared transformation utilities.

- `sql_client.py`  
  Streamlit-based SQL client for querying Parquet data with DuckDB.

- `pipeline_lib/sql/`  
  SQL query templates and logic for OLAP export and reporting.

---

## Installation & Setup

1. **Install Python dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

2. **Configure environment variables:**
   - Copy or edit `folderpath.env.local` to set paths such as `RAWDATA_ROOT_PATH` and `PIPELINE_ROOT_PATH`.

3. **Set up required directory structure:**
   - The pipeline expects the following structure (see `pipeline_lib/config.py` for details):

     ```
     RAWDATA_ROOT_PATH/
       <projectid>_<projectfullname>/
         WE <yyyy.mm.dd>/
           ...
     PIPELINE_ROOT_PATH/
       Data_Process/
         queues/
         logs/
       Data_Transformed/
         Parquet/
           <projectid>_<project-name>/
             <yyyy-mm-dd>/
       OLAP_Export/
         <projectid>_<project-name>/
           <yyyy-mm-dd>/
         project_masterfile.xlsx
         snapshot_rawdata_folders.csv
     ```

---

## Usage

### Run the main pipeline (incremental processing):

```bash
python main.py --auto
```

#### Main pipeline options (from `main.py`):

- `--auto`      Run all steps of the pipeline
- `--snapshot`  Only generate the raw data snapshot
- `--enqueue`   Only compare last snapshots and enqueue new items
- `--transform` Only transform enqueued items
- `--olap`      Only sync OLAP exports
- `--pbi`       Only refresh Power BI dataset

### Example: Generate only the raw data snapshot

```bash
python main.py --snapshot
```

---

## Configuration

All configuration options (paths, columns, etc.) are managed in `pipeline_lib/config.py` and loaded from `folderpath.env.local`.

---

## Dependencies

All required packages are listed in `requirements.txt`. Key dependencies include:

- `pandas`
- `numpy`
- `duckdb`
- `openpyxl`
- `python-dotenv`
- `streamlit`
- `requests`
- `altair`
- (see `requirements.txt` for full list)

---

## Extending the Pipeline

- Add new transformation logic in `pipeline_lib/transform_rawdata.py` and `pipeline_lib/project_transformers/`.
- Add new query for PowerBI exports in `pipeline_lib/sql/`.

---

## Troubleshooting

- All logging is written to the rotating log file defined in `pipeline_lib/logging_config.py` (default: `pipeline.log` in the logs directory).
- Ensure all required environment variables are set in `folderpath.env.local`.

---
