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
,

-- REPORT
label_error_count AS (
    SELECT week_ending, project_id, rubric, 'Incorrect Annotation' as rater_response, 'None' as ground_truth, COUNT(*) AS error_count
    FROM alldata
    WHERE NOT job_correct
    GROUP BY week_ending, project_id, rubric
),

label_error_contribution AS (
    SELECT 
        *, 
        SUM(error_count) OVER (PARTITION BY week_ending, project_id, rubric)::INT AS weekly_label_error_count,
        error_count / SUM(error_count) OVER (PARTITION BY week_ending, project_id, rubric)::FLOAT AS weekly_error_contribution
    FROM label_error_count
),

label_cumulative_error_contribution AS (
    SELECT 
        *,
        SUM(weekly_error_contribution) OVER (
          PARTITION BY week_ending, project_id, rubric
          ORDER BY weekly_error_contribution DESC
          ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS cumulative_error_contribution
    FROM label_error_contribution
)


SELECT * FROM label_cumulative_error_contribution
ORDER BY week_ending, project_id, rubric, cumulative_error_contribution ASC