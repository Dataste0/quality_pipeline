import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from pipeline_lib.pipeline_utils import hash_header
import pipeline_lib.config as cfg

FILEPATH = os.path.join(
    cfg.RAWDATA_ROOT_PATH, 
    "a01Hs00001ocUZnIAM_Marketplace Contextual Ads", "WE 2025.06.27", 
    "Murray_WE06.27.2025_UQD HALO.csv")

def main():
    
    hash_header_value = hash_header(FILEPATH)

    print(f"File: {FILEPATH}")
    print(f"Header Hash: {hash_header_value}")


main()


