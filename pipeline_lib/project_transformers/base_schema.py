# List of columns that all transformed DataFrames must include

universal_quality_columns = [
    "project_id",
    "workflow",
    "job_id",
    "question_number",
    "actor_id",
    "judgment_id",
    "submission_date",
    "is_audit",
    "source_of_truth",
    "parent_label",
    "response_data",
    "recall_precision",
    "weight"
]

halo_rubric_columns = [
    "project_id",
    "workflow",
    "submission_date",
    "job_id",
    
    "rater_id",
    "auditor_id",

    "final_job_tag",
    "final_job_score",
    
    "rubric", # penalty names
    "factor",
    "rubric_score"
]

halo_audits_columns = [
    "project_id",
    "submission_date",
    "job_id",
    "rater_id",
    "auditor_id",
    "parent_label",
    "rater_response",
    "auditor_response",
    "is_correct",
    "confusion_type",
    "weight"
]

common_columns = [
    "com_rawdata_file",
    "com_reporting_week",
    "com_content_week",
    "com_project_id"
]