-- Dedupe
WITH alldata AS (
    SELECT 
        *, 
        reporting_week as week_ending
    FROM (
        SELECT *, 
               ROW_NUMBER() OVER (
                   PARTITION BY project_id, job_id, rater_id, auditor_id, job_correct
               ) AS row_num
        FROM {input_path}
        WHERE 1
        AND job_correct IS NOT NULL
    ) t
    WHERE row_num = 1
)


SELECT 
    week_ending,
    project_id,
    workflow,
    rater_id,
    job_id,
    'default_label' as parent_label,
    'n/a' as rater_response,
    'Incorrect Annotation' as ground_truth,
    '' as confusion_type,
    job_correct as is_correct
FROM alldata
GROUP BY week_ending, project_id, workflow, rater_id, job_id, job_correct
HAVING job_correct IS FALSE
LIMIT 500
