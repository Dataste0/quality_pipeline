import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from pipeline_lib.pipeline_utils import hash_header
import pipeline_lib.config as cfg
import pipeline_lib.pipeline_utils as pu

PROJECT_ID = "a01Hs00001ocUaAIAU"

# --- Setup Project List
PROJECT_MASTERFILE = cfg.PROJECT_INFO_FILE_PATH
project_list_df = pu.load_project_info(PROJECT_MASTERFILE, active_only=False)


def main():
    project_id = PROJECT_ID
    project_rawdata_path = pu.get_project_folder(project_id, cfg.RAWDATA_ROOT_PATH)["path"]

    # select row in project_list_df where project_id matches project_id using loc
    project_rows = project_list_df.loc[project_list_df["project_id"] == project_id]

    if project_rows.empty:
        print(f"No project found for ID {project_id}")
        return False
    
    project_row = project_rows.iloc[0]
    print(f"Project: {project_id} ({project_row['project_name']})")

    data_weeks = pu.generate_we_dates(project_row["project_start_date"])
    for data_week in data_weeks:
        data_week = f"WE {data_week.strftime('%Y.%m.%d')}"

        # list dir
        week_dir = os.path.join(project_rawdata_path, data_week)
        if not os.path.exists(week_dir):
            continue

        # list week dir
        week_files = os.listdir(week_dir)
        for file in week_files:
            if file.endswith(".csv") or file.endswith(".xlsx") or file.endswith(".xls"):
                filepath = os.path.join(week_dir, file)
                headerhash = pu.hash_header(filepath)
                print(f"Project: {project_id} ({project_row['project_name']}) | {data_week} | File: {file} | Header: {headerhash}")
            else:
                continue

main()


