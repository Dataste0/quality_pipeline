WITH alldata AS (
    SELECT 
        *, 
        reporting_week as week_ending
    FROM (
        SELECT *, 
               ROW_NUMBER() OVER (
                   PARTITION BY project_id, job_id, rater_id, parent_label
               ) AS row_num
        FROM {input_path}
        WHERE parent_label IS NOT NULL 
          AND parent_label <> '' 
          AND parent_label <> 'pipeline_error'
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
    parent_label,
    rater_response,
    auditor_response as ground_truth,
    confusion_type,
    is_correct
FROM alldata
WHERE NOT is_correct OR is_correct = 0
LIMIT 500
