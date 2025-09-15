-- Dedupe
WITH alldata AS (
    SELECT 
        *, 
        reporting_week as week_ending
    FROM (
        SELECT *, 
               ROW_NUMBER() OVER (
                   PARTITION BY project_id, job_id, rater_id, auditor_id, job_correct, rubric
               ) AS row_num
        FROM {input_path}
        WHERE 1
        AND job_correct IS NOT NULL
    ) t
    WHERE row_num = 1
)


-- REPORT
SELECT
    project_id,
    week_ending,
    workflow,
    rater_id,
    auditor_id,
    job_id,
    job_correct,
    job_manual_score,
    rubric,
    factor,
    rubric_penalty,
    rubric_score
FROM alldata