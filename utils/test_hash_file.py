import os
import sys
from pathlib import Path
import shutil

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import pipeline_lib.pipeline_utils as pu
import pipeline_lib.config as cfg

RAWDATA_BASE_PATH = cfg.RAWDATA_ROOT_PATH

TESTFILE_PATH = os.path.join(RAWDATA_BASE_PATH, "a01TR00000MGcpSYAT_MGenAI Video Expansion", "WE 2025.05.02", "REGULAR QUEUE 52.csv")

get_hash_value = pu.hash_file(TESTFILE_PATH)

print(f"\n[HASHTEST] Hash value for {TESTFILE_PATH}: {get_hash_value}")

