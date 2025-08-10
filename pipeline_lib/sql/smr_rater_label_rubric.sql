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
-- Jobs correct
rater_correct_jobs_labels AS (
    SELECT 
        week_ending, 
        project_id, 
        workflow, 
        rater_id, 
        job_id,
        'default_rubric' as parent_label,
        MAX(job_score) as job_score,
        MAX(job_correct) as job_correct
    FROM alldata
    WHERE job_correct IS NOT NULL 
    AND auditor_id IS NOT NULL 
    AND auditor_id <> ''
    GROUP BY week_ending, project_id, workflow, rater_id, job_id
),

rater_correct_jobs AS (
    SELECT 
        week_ending, 
        project_id, 
        workflow, 
        rater_id, 
        parent_label,
        COUNT(job_id) as tot_labels,
        SUM(CASE WHEN job_correct THEN 1 ELSE 0 END) as correct_labels,
        AVG(job_score) as rater_label_score,
        0::INT AS tp_count,
        0::INT AS tn_count,
        0::INT AS fp_count,
        0::INT AS fn_count
    FROM rater_correct_jobs_labels
    GROUP BY week_ending, project_id, workflow, rater_id, parent_label
)
,
rater_label_score AS (
    SELECT
        *,
        {target}::FLOAT AS target_goal,
        0::FLOAT AS rater_label_f1score,
        0::FLOAT AS rater_label_precision,
        0::FLOAT AS rater_label_recall
    FROM rater_correct_jobs
)
,
rater_info AS (
    SELECT 
        week_ending,
        project_id,
        workflow,
        rater_id,
        parent_label,
        tot_labels,
        correct_labels,
        tp_count,
        tn_count,
        fp_count,
        fn_count,
        target_goal,
        rater_label_score,
        rater_label_f1score,
        rater_label_precision,
        rater_label_recall,
        AVG(rater_label_score) OVER (PARTITION BY week_ending, project_id, workflow, rater_id) as rater_score,
        AVG(rater_label_f1score) OVER (PARTITION BY week_ending, project_id, workflow, rater_id) as rater_f1score,
        AVG(rater_label_precision) OVER (PARTITION BY week_ending, project_id, workflow, rater_id) as rater_precision,
        AVG(rater_label_recall) OVER (PARTITION BY week_ending, project_id, workflow, rater_id) as rater_recall
    FROM rater_label_score
)

SELECT * FROM rater_info