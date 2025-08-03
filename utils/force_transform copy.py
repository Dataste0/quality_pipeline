import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import pipeline_lib.config as cfg
import pipeline_lib.pipeline_utils as pu
from pipeline_lib.project_transformers.base_audit import transform

RAWFILE_PATH = os.path.join(
    cfg.RAWDATA_ROOT_PATH, 
    "a01TR00000NBVZbYAP_Promotional Ads Coupon Accuracy", "WE 2025.06.06", 
    "Baikal_HA Data__WE 2025.06.06.csv")

METADATA = {
    "project_id" : "a01TR00000NBVZbYAP",
    "project_name" : "Promotional Ads Coupon Accuracy",
    "module" : "HALO-RUBRIC",
    "model_base": "audit",
    "mod_config" : {
        
        "metadata_columns": {
            "rater_id_column": "Annotator ID",
            "auditor_id_column": "Vendor Auditor ID",
            "job_id_column": "SRT Job ID",
            "submission_date_column": "Time (PT)",
            "workflow_column": "Rubric Name",
            "final_job_tag_column": "Vendor Tag",
            "manual_score_column": "Vendor Manual QA Score"
        },

        "default_label_name": "rubric_default",
        "top_score": 100,
        "bottom_score": 80,
        "use_score_provided": False,
        
        "rubric": [
            {
                "label_column" : "Conditions - Incorrect", 
                "label_replace" : "conditions_incorrect",
                "penalty_score": 0.05
            },
            {
                "label_column" : "Can Promotion Be Applied - Incorrect", 
                "label_replace" : "can_promotions_be_applied_incorrect",
                "penalty_score": 0.05
            },
            {
                "label_column" : "Promotion Apply Result Message - Incorrect/Missing", 
                "label_replace" : "promotion_apply_result_incorrect",
                "penalty_score": 0.05
            },
            {
                "label_column" : "Cannot Validate Reason - Incorrect", 
                "label_replace" : "cannot_validate_reason_incorrect",
                "penalty_score": 0.05
            },
            {
                "label_column" : "Job Incorrectly Rejected", 
                "label_replace" : "job_incorrectly_rejected",
                "penalty_score": 0.2
            }
        ]
    }
}

df = pu.load_df_from_filepath(RAWFILE_PATH)

df_out, info = process_dataframe(df, METADATA)

print(f"INFO DICT: {info}")

df_out.to_csv('test_module.csv', index=False)