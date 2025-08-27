# quality_pipeline

A modular data pipeline for processing, transforming, and preparing raw data for DataOps ingestion and OLAP export.

## Overview

This project provides a set of scripts and utilities to automate the ingestion, transformation, and export of raw data files for analytical processing. The pipeline is designed to support both incremental updates and historical backfill operations.

## Main Components

- **main.py**  
  The primary pipeline script. Scans for new raw data, transforms it, and prepares it for DataOps ingestion.

- **backfill.py**  
  Processes all historical data for selected projects, transforming and ingesting it in bulk.

- **olap_update.py**  
  Processes transformed data for OLAP (Online Analytical Processing) export after DataOps ingestion.

## Workflow

1. **Load Project Metadata:**  
   Project information is loaded from a master file.

2. **Scan Raw Data:**  
   The pipeline scans raw data folders for new or updated files.

3. **Snapshot & Compare:**  
   Snapshots of raw data are compared to identify new entries.

4. **Transform Data:**  
   New entries are transformed for downstream processing.

5. **Ingest & Export:**  
   Transformed data is moved to the DataOps ingestion folder. OLAP exports are triggered separately.

## Usage

### Incremental Pipeline

Run the main pipeline to process new data:

```bash
python main.py
```

### Backfill Historical Data

To process all historical data for specific projects, edit `BACKFILL_PROJECT_IDS` in `backfill.py` and run:

```bash
python backfill.py
```

### OLAP Export

After DataOps ingestion is complete, run:

```bash
python olap_update.py
```

## Configuration

All configuration options (paths, columns, etc.) are managed in `pipeline_lib/config.py`.

## Buffer Management

The pipeline uses CSV-based buffer managers to track snapshots, enqueued, and transformed data:

- `RAWADATA_LOG_FILE_PATH`
- `ENQUEUED_LOG_FILE_PATH`
- `TRANSFORMED_LOG_FILE_PATH`

## Extending

- Add new transformation logic in `pipeline_lib/transform_rawdata.py`.
- Add new OLAP export logic in `pipeline_lib/olap_process.py`.

## Notes

- OLAP export should be triggered only after DataOps has ingested the transformed data.
- The pipeline supports both active and inactive projects for backfill.

---