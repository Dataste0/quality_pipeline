-- Dedupe
WITH alldata AS (
    SELECT 
        *, 
        reporting_week as week_ending
    FROM (
        SELECT *, 
               ROW_NUMBER() OVER (
                   PARTITION BY project_id, job_id, rater_id, rubric
               ) AS row_num
        FROM {input_path}
        WHERE rubric IS NOT NULL 
          AND rubric <> '' 
          AND rubric <> 'pipeline_error'
          AND rubric_score = 1
          AND project_id = {project_id}
          AND reporting_week = {reporting_week}
    ) t
    WHERE row_num = 1
)


SELECT 
    week_ending,
    project_id,
    workflow,
    rater_id,
    job_id,
    'Incorrect Annotation' as rubric,
    '' as rater_response,
    '' as ground_truth,
    '' as confusion_type,
    job_correct as is_correct
FROM alldata
WHERE NOT job_correct OR job_correct = 0
LIMIT 500
