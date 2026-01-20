import os
import sys
import pandas as pd
import shutil

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import pipeline_lib.config as cfg
import pipeline_lib.pipeline_utils as pu

# --- Setup Project List
PROJECT_MASTERFILE = cfg.PROJECT_INFO_FILE_PATH
project_list_df = pu.load_project_info(PROJECT_MASTERFILE, active_only=False)


def archive_old_folders():
    project_df = project_list_df

    total = len(project_df)

    default_ts = pd.to_datetime(cfg.START_DATE_DEFAULT, errors="coerce")
    if pd.isna(default_ts):
        raise ValueError("cfg.START_DATE_DEFAULT is missing or not parseable")
    default_date = default_ts.date()

    for idx, row in project_df.iterrows():
        print(f"Processing project {idx+1}/{total}: {row.get('project_id')} ({row.get('project_name')})")

        project_id = row.get("project_id")
        project_folder = pu.get_project_folder(project_id, cfg.RAWDATA_ROOT_PATH)

        if not project_folder.get("exists", False):
            print(f"  - Project folder does not exist: {project_folder.get('path')}")
            continue

        folder_path = project_folder["path"]
        print(f"  - Project folder found: {folder_path}")

        archive_path = os.path.join(cfg.RAWDATA_ROOT_PATH, "Archive", project_folder["name"])
        os.makedirs(archive_path, exist_ok=True)

        try:
            entries = os.listdir(folder_path)
        except OSError as e:
            print(f"  - Cannot list folder: {folder_path} ({e})")
            continue

        for name in entries:
            we_folder_path = os.path.join(folder_path, name)
            if not (os.path.isdir(we_folder_path) and name.startswith("WE ")):
                continue

            # Parse date after "WE "
            we_date_str = name[3:].strip()

            # First try strict expected format, fallback to flexible parse
            we_ts = pd.to_datetime(we_date_str, format="%Y.%m.%d", errors="coerce")
            if pd.isna(we_ts):
                we_ts = pd.to_datetime(we_date_str, errors="coerce")

            if pd.isna(we_ts):
                print(f"    - Skipping (unparseable date): {name}")
                continue

            we_date = we_ts.date()

            if we_date < default_date:
                dest = os.path.join(archive_path, name)

                # Avoid collision
                if os.path.exists(dest):
                    print(f"    - Destination exists, skipping: {dest}")
                    continue

                try:
                    shutil.move(we_folder_path, dest)
                    print(f"    - Archived: {name} -> {dest}")
                except OSError as e:
                    print(f"    - Failed to move {we_folder_path} -> {dest} ({e})")


if __name__ == "__main__":
    archive_old_folders()
