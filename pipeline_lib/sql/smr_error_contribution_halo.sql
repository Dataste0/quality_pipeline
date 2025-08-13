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
,

-- REPORT
label_error_count AS (
    SELECT week_ending, project_id, 'default_label' AS parent_label, 'n/a' as rater_response, 'Incorrect Annotation' as ground_truth, COUNT(*) AS error_count
    FROM alldata
    WHERE job_correct IS FALSE
    GROUP BY week_ending, project_id
),

label_error_contribution AS (
    SELECT 
        week_ending, 
        project_id, 
        parent_label, 
        COALESCE(NULLIF(rater_response, ''), '<empty>') as rater_response, 
        COALESCE(NULLIF(ground_truth, ''), '<empty>') as ground_truth, 
        error_count,
        SUM(error_count) OVER (PARTITION BY week_ending, project_id, parent_label)::INT AS weekly_label_error_count,
        error_count / SUM(error_count) OVER (PARTITION BY week_ending, project_id, parent_label)::FLOAT AS weekly_error_contribution
    FROM label_error_count
)


SELECT * 
FROM label_error_contribution