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
,

-- REPORT
label_error_count AS (
    SELECT week_ending, project_id, parent_label, rater_response, auditor_response as ground_truth, COUNT(*) AS error_count
    FROM alldata
    WHERE NOT is_correct OR is_correct = 0
    GROUP BY week_ending, project_id, parent_label, rater_response, auditor_response
),

label_error_contribution AS (
    SELECT 
        *, 
        SUM(error_count) OVER (PARTITION BY week_ending, project_id, parent_label)::INT AS weekly_label_error_count,
        error_count / SUM(error_count) OVER (PARTITION BY week_ending, project_id, parent_label)::FLOAT AS weekly_error_contribution
    FROM label_error_count
),

label_cumulative_error_contribution AS (
    SELECT 
        *,
        SUM(weekly_error_contribution) OVER (
          PARTITION BY week_ending, project_id, parent_label
          ORDER BY weekly_error_contribution DESC
          ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS cumulative_error_contribution
    FROM label_error_contribution
)


SELECT * FROM label_cumulative_error_contribution
ORDER BY week_ending, project_id, parent_label, cumulative_error_contribution ASC