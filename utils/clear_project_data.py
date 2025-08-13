import os
import sys
from pathlib import Path
import shutil

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import pipeline_lib.config as cfg
TRANSFORMED_BASE_PATH = cfg.DATA_TRANSFORMED_DIR_PATH
OLAP_BASE_PATH = cfg.OLAP_EXPORT_DIR_PATH


PROJECT_IDS = ["a01Hs00001ocUaxIAE",
               "a01Hs00001ocUZsIAM",
               "a0ATR000001slqz2AA",
               "a01TR00000Eb8KKYAZ",
               "a01TR00000QHbkAYAT",
               "a01TR00000NBVZbYAP"
               ]




# delete project_id folders
def delete_project_folders(project_id):
    transformed_path = Path(TRANSFORMED_BASE_PATH) / project_id
    olap_path = Path(OLAP_BASE_PATH) / project_id

    if transformed_path.exists():
        print(f"Deleting transformed data for project {project_id}")
        for item in transformed_path.glob("*"):
            if item.is_dir():
                shutil.rmtree(item)
            else:
                item.unlink()

    if olap_path.exists():
        print(f"Deleting OLAP data for project {project_id}")
        for item in olap_path.glob("*"):
            if item.is_dir():
                shutil.rmtree(item)
            else:
                item.unlink()


for project_id in PROJECT_IDS:
    delete_project_folders(project_id)