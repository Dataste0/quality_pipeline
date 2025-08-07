import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from pipeline_lib.pipeline_utils import hash_header
import pipeline_lib.config as cfg

FILEPATH = os.path.join(
    cfg.RAWDATA_ROOT_PATH, 
    "01TR00000KeKFrYAN_MVP-Generative composition", "WE 2025.05.16", 
    "WE 05.16.csv")

def main():
    
    hash_header_value = hash_header(FILEPATH)

    print(f"File: {FILEPATH}")
    print(f"Header Hash: {hash_header_value}")


main()


