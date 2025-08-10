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
    'Incorrect Annotation' as parent_label,
    'n/a' as rater_response,
    'n/a' as ground_truth,
    '' as confusion_type,
    job_correct as is_correct
FROM alldata
GROUP BY week_ending, project_id, workflow, rater_id, job_id, job_correct
HAVING NOT job_correct
LIMIT 500
