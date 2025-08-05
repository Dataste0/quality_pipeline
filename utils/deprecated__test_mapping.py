import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import pipeline_lib.config as cfg
import pipeline_lib.pipeline_utils as pu
from pipeline_lib.sql.query_map_info import query_map_info_run
import pandas as pd
from openpyxl import load_workbook

MARKET_MAP_FILE_PATH = os.path.join(cfg.RAWDATA_ROOT_PATH, "project_info_market_map.xlsx")
LABEL_MAP_FILE_PATH = os.path.join(cfg.RAWDATA_ROOT_PATH, "project_info_label_map.xlsx")

PROJECT_MASTERFILE = cfg.PROJECT_INFO_FILE_PATH

# --- Setup Project List
project_list_df = pu.load_project_info(PROJECT_MASTERFILE, active_only=False)



query_map_info_run()
