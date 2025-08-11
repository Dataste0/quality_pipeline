import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import pipeline_lib.config as cfg
import pipeline_lib.pipeline_utils as pu
from pipeline_lib.project_transformers.mod_uqd import transform as trs_uqd
from pipeline_lib.project_transformers.mod_cvs import transform as trs_cvs
from quality_pipeline.pipeline_lib.project_transformers.deprecated_mod_halo import transform as trs_halo

UQDMULTI_RAWFILE_PATH = os.path.join(
    cfg.RAWDATA_ROOT_PATH, 
    "a01Hs00001ocUiNIAU_GenAds Uncropping", "WE 2025.04.25",
    "GenAds Uncropping Standalone WE 4.25.25.csv"    
)
UQDAUDIT_RAWFILE_PATH = os.path.join(
    cfg.RAWDATA_ROOT_PATH, 
    "a01TR00000MBZKQYA5n1_Objective Quality for Threads-Bait", "WE 2025.07.11",
    "JP-Bait- Objective Quality for Threads (Calathea) - 07.11.csv"    
)

CVSAUDIT_RAWFILE_PATH = os.path.join(
    cfg.RAWDATA_ROOT_PATH, 
    "a01Hs00001ocUa0IAE_Clickbait V2", "WE 2025.06.13",
    "cvs_cb_withhold_raw_labelling_info_2025-06-16.csv"    
)

HALORUBRIC_RAWFILE_PATH = os.path.join(
    cfg.RAWDATA_ROOT_PATH, 
    "a01TR00000Eb8KKYAZ_Image Generation Human Flaw Understanding", "WE 2025.07.04",
    "WE0704 Image Generation Human Flaw Raw Data.xlsx"    
)


METADATA_AUDIT = {
    "project_id" : "dummy1",
    "project_name" : "Dummy test",
    "module" : "CVS",
    "model_config" : {
        
        "info_columns": {
            "rater_id_column": "rater-id",
            "auditor_id_column": "auditor-id",
            "job_id_column": "job-id",
            "submission_date_column": "date-id",
            #"workflow_column": "Queue Name",
            #"provided_job_outcome_column": "Is Correct",
            #"provided_job_score_column": "Job Score"
        },

        "use_job_outcome_provided": False,
        "use_job_score_provided": False,
        
        "labels": [
            {
                "label_name": "able_to_eval",
                "rater_label_column": "rater Q1 able to evaluate", 
                "auditor_label_column": "Q1 correctness",
                "auditor_column_type": "agreement",
                "is_label_binary": True,
                "label_binary_pos_value": "EB_yes",
                #"weight": ""
            },
            {
                "label_name": "sticky_ads",
                "rater_label_column": "rater Q2 sticky ads", 
                "auditor_label_column": "auditor Q2",
                "auditor_column_type": "answer",
                "is_label_binary": False,
                "weight": "0.8"
            }
        ]
    }
}

METADATA_MULTI = {
    "project_id" : "dummy1",
    "project_name" : "Dummy test",
    #"module" : "HALO-RUBRIC",
    "model_base": "multi", #/rubric/multi
    "mod_config" : {
        
        "info_columns": {
            "rater_id_column": "rater-id",
            "job_id_column": "job-id",
            "submission_date_column": "date-id",
            #"workflow_column": "Queue Name",
            #"provided_job_outcome_column": "Is Correct",
            #"provided_job_score_column": "Job Score"
        },
        
        "labels": [
            {
                "label_name": "able_to_eval",
                "rater_label_column": "rater Q1 able to evaluate", 
                "is_label_binary": True,
                "label_binary_pos_value": "EB_yes",
                #"weight": ""
            },
            {
                "label_name": "sticky_ads",
                "rater_label_column": "rater Q2 sticky ads", 
                "weight": "0.8"
            }
        ]
    }
}

METADATA_UQD_MULTI = {
    "project_id" : "dummy1",
    "project_name" : "Dummy test",
    "module" : "UQD",
    "mod_config" : {
        "use_extracted": True,
        "quality_methodology": "multi",
        "excluded_labels": ["outcropping_standalone_issue"]
    }
}

METADATA_UQD_AUDIT = {
    "project_id" : "dummy1",
    "project_name" : "Dummy test",
    "module" : "UQD",
    "mod_config" : {
        "use_extracted": True,
        "quality_methodology": "audit",
        #"excluded_labels": []        
    }
}

METADATA_CVS_AUDIT = {
    "project_id" : "dummy1",
    "project_name" : "Dummy test",
    "module" : "CVS",
    "mod_config" : {
        #"quality_methodology": "audit",
        #"excluded_labels": []        
    }
}

METADATA_HALO_RUBRIC = {
    "project_id" : "dummy1",
    "project_name" : "Dummy test",
    "module" : "HALO",
    "mod_config" : {
        
        "info_columns": {
            "rater_id_column": "Annotator ID",
            "auditor_id_column": "Vendor Auditor ID",
            "job_id_column": "SRT Job ID",
            "submission_date_column": "Time (PT)",
            "workflow_column": "Rubric Name",
            "vendor_tag_column": "Vendor Tag",
            "manual_score_column": "Vendor Manual QA Score"
        },

        "top_score": 100,
        "bottom_score": 80,
        "default_label_name": "main_rubric",
        "use_job_score_provided": True,
        #"use_job_outcome_provided": True,
        
        "rubric": [
            {
                "label_column" : 'Rater selected "Body distortion" when there was no flaw.', 
                "label_name" : "incorrect_body_distortion",
                "penalty_score": 0.05
            },
            {
                "label_column" : 'Rater selected "Eye related flaws" when there was no flaw.', 
                "label_name" : "incorrect_eye_related_flaws",
                "penalty_score": 0.05
            },
            {
                "label_column" : 'Rater makes any incorrect selection on Q1.', 
                "label_name" : "incorrect_selection_q1",
                "penalty_score": 0.05
            },
            {
                "label_column" : 'Rater should have selected "Hand-related flaws" but did not.', 
                "label_name" : "hand_related_flaws_not_selected",
                "penalty_score": 0.05
            },
            {
                "label_column" : 'Rater should have selected "Eye-related flaws" but did not.', 
                "label_name" : "eye_related_flaws_not_selected",
                "penalty_score": 0.05
            },
            {
                "label_column" : 'Rater selected "Face-related flaws" when there was no flaw.', 
                "label_name" : "face_related_flaws_with_no_flaw",
                "penalty_score": 0.05
            },
            {
                "label_column" : 'Rater should have selected "Face-related flaws" but did not.', 
                "label_name" : "face_related_flaws_not_selected",
                "penalty_score": 0.05
            }
            ,
            {
                "label_column" : 'Rater should have selected "Body distortion" but did not.', 
                "label_name" : "body_distortion_not_selected",
                "penalty_score": 0.05
            }
            ,
            {
                "label_column" : 'Rater selected "Hand-related flaws" when there was no flaw.', 
                "label_name" : "hand_related_flaws_with_no_flaw",
                "penalty_score": 0.05
            }
        ]
    }
}


df = pu.load_df_from_filepath(HALORUBRIC_RAWFILE_PATH)

df_out, info = trs_halo(df, METADATA_HALO_RUBRIC)

print(f"INFO DICT: {info}")

df_out.to_csv('force_transform_output.csv', index=False)