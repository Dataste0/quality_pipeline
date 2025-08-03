import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from pipeline_lib.pipeline_utils import hash_header
import pipeline_lib.config as cfg

FILEPATH = os.path.join(
    cfg.RAWDATA_ROOT_PATH, 
    "a01Hs00001ocUa0IAE_Clickbait V2", "WE 2025.07.11", 
    "cvs_cb_withhold_raw_labelling_info_2025-07-14.csv")

def main():
    
    hash_header_value = hash_header(FILEPATH)

    print(f"File: {FILEPATH}")
    print(f"Header Hash: {hash_header_value}")


main()


