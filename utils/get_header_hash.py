import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from pipeline_lib.pipeline_utils import hash_header
import pipeline_lib.config as cfg

FILEPATH = os.path.join(
    cfg.RAWDATA_ROOT_PATH, 
    "a0ATR000001slqz2AA_Concept relation annotation", 
    "WE 2025.07.04", 
    "An9CMg6VK0rInYnzFLEfB8mrr49b17uT3iq04q2ydhqW1Z87pJNMxKlTVxB-DZY5Fha0PhmP7SxSU4_a7cE8AwVKZxHRCcueGKDDFwyclf5B2YU (1).csv"
)

def main():
    
    hash_header_value = hash_header(FILEPATH)

    print(f"File: {FILEPATH}")
    print(f"Header Hash: {hash_header_value}")


main()


