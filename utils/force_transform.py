import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import pipeline_lib.config as cfg
import pipeline_lib.pipeline_utils as pu
import pipeline_lib.project_transformers.mod_agreement_unpivot as mod_au

RAWFILE_PATH = os.path.join(cfg.RAWDATA_ROOT_PATH, "a01Hs00001q1a2WIAQ_Crawling Structured Description", "WE 2025.04.18", "annotation_structured_description_all_last_day_audits_2025-04-14.csv")

METADATA = {
    "project_id" : "blabla",
    "project_name" : "abc",
    "module" : "AU",
    "mod_config" : {
        "metadata_columns": {
            "rater_id_column_name": "Annotator ID",
            "auditor_id_column_name": "Auditor ID",
            "job_id_column_name": "Annotation Job ID",
            "submission_date_column_name": "Annotation Date And Time"
        },
        
        "label_columns": {
            "label_0": {"label_name" : "KPI: Is URL classified correctly?", "label_replace" : "URL classified correctly"},
            "label_1": {"label_name" : "KPI: Is Title annotated correctly?", "label_replace" : "Title annotated correctly"},
            "label_2": {"label_name" : "KPI: Is main-description annotated correctly?", "label_replace" : "main-description annotated correctly"},
            "label_3": {"label_name" : "KPI: Is additional-description annotated correctly?", "label_replace" : "additional-description annotated correctly"}
        }
    }
}

df = pu.load_df_from_filepath(RAWFILE_PATH)

df_out = mod_au.transform(df, METADATA)

df_out.head(10).to_csv('test_module.csv')