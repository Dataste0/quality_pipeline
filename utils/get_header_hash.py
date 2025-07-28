import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from pipeline_lib.pipeline_utils import hash_header
import pipeline_lib.config as cfg

FILEPATH = os.path.join(cfg.RAWDATA_ROOT_PATH, "a01Hs00001q1a2WIAQ_Crawling Structured Description", "WE 2025.04.18", "annotation_structured_description_all_last_day_audits_2025-04-14.csv")

def main():
    
    hash_header_value = hash_header(FILEPATH)

    print(f"File: {FILEPATH}")
    print(f"Header Hash: {hash_header_value}")


main()


