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
,

-- REPORT
job_label_correctness AS (
    SELECT 
        week_ending,
        project_id,
        workflow,
        job_id,
        rubric,
        COUNT(*) AS rater_count,
        SUM(job_correct) AS correct_rater_count,
        AVG(job_score) AS job_score,
        0::INT AS tp_count,
        0::INT AS tn_count,
        0::INT AS fp_count,
        0::INT AS fn_count
    FROM alldata
    GROUP BY week_ending, project_id, workflow, job_id, rubric
),

job_label_score AS (
    SELECT 
        week_ending,
        project_id,
        workflow,
        job_id,
        rubric,
        rater_count,
        correct_rater_count,
        tp_count,
        tn_count,
        fp_count,
        fn_count,
        job_score::FLOAT AS job_label_score,
        0::FLOAT AS job_label_f1score,
        0::FLOAT AS job_label_precision,
        0::FLOAT AS job_label_recall
    FROM job_label_correctness
)

SELECT *
FROM job_label_score