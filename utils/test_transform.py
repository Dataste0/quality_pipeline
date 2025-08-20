import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import pandas as pd
import pipeline_lib.config as cfg
import pipeline_lib.pipeline_utils as pu
from pipeline_lib.project_transformers.dispatcher import process_dataframe

RAW_GENERIC_MULTI_UNPIV = os.path.join(
    cfg.RAWDATA_ROOT_PATH,
    "a01Hs00001q1a2VIAQ_Crawling Correctness-Media", "WE 2025.07.18",
    "media_appen_all_answers_prod_daily_2025-07-17.csv"
)

RAW_GENERIC = os.path.join(
    cfg.RAWDATA_ROOT_PATH,
    "a01Hs00001q1a2WIAQ_Crawling Structured Description", "WE 2025.07.04",
    "annotation_structured_description_all_last_day_audits_2025-07-04.csv"
)

RAWFILE_PATH = os.path.join(
    cfg.RAWDATA_ROOT_PATH, 
    "a01Hs00001q1a2WIAQ_Crawling Structured Description", "WE 2025.07.04", 
    "annotation_structured_description_all_last_day_audits_2025-07-04.csv")

RAW_CVS = os.path.join(
    cfg.RAWDATA_ROOT_PATH, 
    "a01Hs00001ocUa0IAE_Clickbait V2", "WE 2025.05.30", 
    "cvs_cb_withhold_raw_labelling_info_2025-06-02.csv")

RAW_UQD_MULTI = os.path.join(
    cfg.RAWDATA_ROOT_PATH, 
    "a01Hs00001ocUiNIAU_GenAds Uncropping", "WE 2025.05.02", 
    "GenAds Uncropping Precision - UQD.csv")

RAW_UQD_AUDIT = os.path.join(
    cfg.RAWDATA_ROOT_PATH, 
    "a01TR00000MBZKQYA5n1_Objective Quality for Threads-Bait", "WE 2025.07.11", 
    "JP-Bait- Objective Quality for Threads (Calathea) - 07.11.csv")

RAW_HALO = os.path.join(
    cfg.RAWDATA_ROOT_PATH, 
    "a01TR00000NBVZbYAP_Promotional Ads Coupon Accuracy", "WE 2025.06.20", 
    "Baikal_HA Data_WE 2025.06.20.csv")

METADATA_GENERIC = {
    "project_id": "test",
    "project_name": "test",
    "project_config": {
        "files_filter": {
            "begins_with": "",
            "contains": ["Objective Quality for Threads"],
            "ends_with": ".csv"
        },
        "dataset_type": "GENERIC",
        "module": "GENERIC",
        "module_config": {
            "quality_methodology": "audit",
            "info_columns": {
                "rater_id_column": "Annotator ID",
                "auditor_id_column": "Auditor ID",
                "job_id_column": "Annotation Job ID",
                "job_date_column": "Annotation Date And Time"
            },
            "labels": [
                {
                    "label_name": "url_classification",
                    "rater_label_column": "Annotation URL Classification",
                    "auditor_label_column": "Audit URL Classification",
                    "auditor_column_type": "answer",        
                },
                {
                    "label_name": "url_classification_correct",
                    "auditor_label_column": "KPI: Is URL classified correctly?",
                    "auditor_column_type": "agreement",
                    "empty_as": False
                },
                {
                    "label_name": "title_annotation_correct",
                    "auditor_label_column": "KPI: Is Title annotated correctly?",
                    "auditor_column_type": "agreement",
                    "empty_as": False
                },
                {
                    "label_name": "main_description_correct",
                    "auditor_label_column": "KPI: Is main-description annotated correctly?",
                    "auditor_column_type": "disagreement",
                    "empty_as": False
                }

            ]
        }
    }
}

METADATA_GENERIC_MULTI_UNPIV = {
    "project_id": "test",
    "project_name": "test",
    "project_config": {
        "files_filter": {
            "begins_with": "",
            "contains": ["Objective Quality for Threads"],
            "ends_with": ".csv"
        },
        "dataset_type": "GENERIC",
        "module": "GENERIC",
        "module_config": {
            "info_columns": {
                    "rater_id_column": "reviewer_id",
                    "job_id_column": "job_id",
                    "_job_date_column": "missing"
            },
            "quality_methodology": "multi",
            "data_structure": "unpivoted",
            "label_column": "question",
            "rater_response_column": "answer"
        }
    }
}

METADATA_CVS = {
  "project_id": "a01TR00000MBZKQYA5n1",
  "project_name": "Objective Quality for Threads - Bait",
  "project_config": [
      {
          "files_filter": {
              "begins_with": "",
              "contains": ["Objective Quality for Threads"],
              "ends_with": ".csv"
          },
          "dataset_fingerprint": "3bc6109e71623b915afbaa7bc7b4a12d",
          "module": "CVS",
          "module_config": {
              "excluded_labels": ["unable_to_evaluate"],
              "binary_labels": [],
          }
      }
  ]
}

METADATA_UQD_MULTI = {
  "project_id": "a01Hs00001ocUiNIAU",
  "project_name": "GenAds Uncropping",
  "project_config": [
      {
          "files_filter": {
              "begins_with": "GenAds Uncropping",
              "contains": [],
              "ends_with": ".csv"
          },
          "dataset_fingerprint": "eaae2c259ef56e33f9310fcc5ddfe850",
          "module": "UQD",
          "module_config": {
              "use_extracted": True,
              "quality_methodology": "multi",
              "excluded_labels": [],
              "binary_labels": [
                {
                  "label_name": "outcropping_standalone",
                  "binary_positive_value": "yes"
                }
              ]
          }      
      }
  ]
}

METADATA_UQD_AUDIT = {
  "project_id": "a01TR00000MBZKQYA5n1",
  "project_name": "Objective Quality for Threads - Bait",
  "project_config": [
      {
          "files_filter": {
              "begins_with": "",
              "contains": ["Objective Quality for Threads"],
              "ends_with": ".csv"
          },
          "dataset_fingerprint": "3bc6109e71623b915afbaa7bc7b4a12d",
          "module": "UQD",
          "module_config": {
              "use_extracted": True,
              "quality_methodology": "audit",
              "excluded_labels": [],
              "binary_labels": []
          }      
      }
  ]
}

METADATA_HALO = {
  "project_id": "a01TR00000NBVZbYAP",
  "project_name": "Promotional Ads Coupon Accuracy",
  "project_config": [
      {
          "files_filter": {
              "begins_with": "Baikal",
              "contains": [],
              "ends_with": ".csv"
          },
          "dataset_fingerprint": "87038ead6061afdce72cb2422c41ef2e",
          "module": "HALO",
          "module_config" : {
              "info_columns": {
                        "rater_id_column": "Annotator ID",
                        "auditor_id_column": "Vendor Auditor ID",
                        "job_id_column": "SRT Job ID",
                        "submission_date_column": "Time (PT)",
                        "workflow_column": "Rubric Name",
                        "vendor_tag_column": "Vendor Tag",
                        "vendor_comment_column": "Vendor Comment",
                        "manual_score_column": "Vendor Manual QA Score"
              },
              
              "bottom_score": 80,
              "default_rubric_name": "main_rubric",
              "use_job_score_provided": True,
              "use_job_outcome_provided": False,
              "incorrect_if_commented": False,
              "pass_score": 100,

              "rubric": [
                  {
                      "rubric_column" : "Conditions - Incorrect", 
                      "rubric_name" : "conditions_incorrect",
                      "penalty_score": 5
                  },
                  {
                      "rubric_column" : "Can Promotion Be Applied - Incorrect", 
                      "rubric_name" : "can_promotions_be_applied_incorrect",
                      "penalty_score": 5
                  },
                  {
                      "rubric_column" : "Promotion Apply Result Message - Incorrect/Missing", 
                      "rubric_name" : "promotion_apply_result_incorrect",
                      "penalty_score": 5
                  },
                  {
                      "rubric_column" : "Cannot Validate Reason - Incorrect", 
                      "rubric_name" : "cannot_validate_reason_incorrect",
                      "penalty_score": 5
                  },
                  {
                      "rubric_column" : "Job Incorrectly Rejected", 
                      "rubric_name" : "job_incorrectly_rejected",
                      "penalty_score": 5
                  }
              ]
          }
      }
  ]
}

METADATA_UQD_NOEXTRACT = {
  "project_id": "a01TR00000Eb8K1YAJ",
  "project_name": "ML Text Generation MAdLlama",
  "project_config": [
      {
          "files_filter": {
              "begins_with": "MAdLlama",
              "contains": [],
              "ends_with": ".csv"
          },
          "dataset_fingerprint": "81528b0bfa44ade9774c67c06c2fbb65",
          "module": "UQD",
          "module_config": {
              "use_extracted": False,
              "quality_methodology": "multi",
              "excluded_labels": ["hallucination_example", "omit_example"],
              "binary_labels": []
          }      
      }
  ]
}

METADATA_MADLLAMA = {
  "project_id": "a01TR00000Eb8K1YAJ",
  "project_name": "ML Text Generation MAdLlama",
  "project_config": {
          "files_filter": {
              "begins_with": "MAdLlama",
              "contains": [],
              "ends_with": ".csv"
          },
          "dataset_type": "UQD-LIKE",
          "module": "UQD",
          "module_config": {
              "replace_columns": [
                {"from": "last_review_ds", "to": "review_ds" }
              ],
              "replace_regex": [
                {"pattern": "(?<=[a-zA-Z])'(?=[a-zA-Z])", "replace": "’", "columns": ["decision_data"] },
                {"pattern": "(?<=s)'(?=[\s\.])", "replace": "’", "columns": ["decision_data"] },
                {"pattern": "(?<=\s)'(?=[a-zA-Z])", "replace": "’", "columns": ["decision_data"] }
              ],
              "use_extracted": False,
              "quality_methodology": "multi",
              "excluded_labels": ["hallucination_example", "omit_example"],
              "binary_labels": []
          }      
    }
}
RAWFILE_UQD_NOEXTRACT = os.path.join(
    cfg.RAWDATA_ROOT_PATH, 
    "a01TR00000Eb8K1YAJ_ML Text Generation MAdLlama", "WE 2025.07.04", 
    "MAdLlama (Adur) Raw data_PT WE07.04.csv")

RAW_MADLLAMA = os.path.join(
    cfg.RAWDATA_ROOT_PATH,
    "a01TR00000Eb8K1YAJ_ML Text Generation MAdLlama", "WE 2025.07.25",
    "MAdLlama (Adur) Raw data_PT WE07.25.csv"
)

RAW_CRAWLING = os.path.join(
    cfg.RAWDATA_ROOT_PATH,
    "a01Hs00001q1a21IAA_Crawling-Interactive Annotation-Variants","WE 2025.07.25",
    "annotation_variants_all_jobs_2025-07-25.csv"
)

METADATA_CRAWLING = {
  "project_id": "test",
  "project_name": "test",
  "project_config": {
          "files_filter": {
              "begins_with": "test",
              "contains": [],
              "ends_with": ".csv"
          },
          "dataset_type": "HALO-LIKE",
          "module": "GENERIC",
          "module_config": {
              "info_columns": {
                    "rater_id_column": "Annotator ID",
                    "auditor_id_column": "Auditor ID",
                    "job_id_column": "Annotation Job ID",
                    "job_date_column": "Annotation Date And Time"
                },
                "quality_methodology": "outcome",
                "outcome_column": "Is Job Successful?",
                "discard_if_empty": True,
                "positive_outcome": "TRUE"
          }      
    }
}


RAW_ADAP = os.path.join(
    cfg.RAWDATA_ROOT_PATH,
    "a01Hs00001ocUa8IAE_IG Off-Topic Relevance Ads PAE", "WE 2025.07.04",
    "Conness ADAP Results Prod WE07.04.2025_Full Report.xlsx"
)

METADATA_ADAP = {
  "project_id": "a01Hs00001ocUa8IAE",
  "project_name": "test",
  "project_config": {
          "files_filter": {
              "begins_with": "test",
              "contains": [],
              "ends_with": ".csv"
          },
          "dataset_type": "ADAP",
          "module": "ADHOC",
          "module_config": {
          }      
    }
}

df = pu.load_df_from_filepath(RAW_ADAP)
project_metadata = METADATA_ADAP
print(f"pandas version: {pd.__version__}")

print(f"\nMetadata {type(project_metadata)}\n{project_metadata}\n")
#project_metadata["project_config"]["module_config"]["reporting_week"] = "1985-01-14"
df_out, stats = process_dataframe(df, project_metadata)

print(f"\nSTATS {stats}")
df_out.to_csv('test_module.csv', index=False)