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
-- Jobs correct
rater_correct_jobs_labels AS (
    SELECT 
        week_ending, 
        project_id, 
        workflow, 
        rater_id,
        job_id,
        1::INT as is_rated,
        1::INT as is_audited,
        CASE WHEN job_correct IS TRUE THEN 1 ELSE 0 END as is_correct,
        CASE WHEN job_correct IS FALSE THEN 1 ELSE 0 END as is_incorrect
    FROM alldata
)
,
rater_correct_jobs AS (
    SELECT 
        week_ending, 
        project_id, 
        workflow, 
        rater_id, 
        'default_label' as parent_label,
        
        SUM(is_audited) as tot_labels,
        SUM(is_correct) as correct_labels,
        
        0::INT AS tp_count,
        0::INT AS tn_count,
        0::INT AS fp_count,
        0::INT AS fn_count
    FROM rater_correct_jobs_labels
    GROUP BY week_ending, project_id, workflow, rater_id
)
,
rater_label_score AS (
    SELECT
        *,
        {target}::FLOAT AS target_goal,
        CASE WHEN tot_labels > 0 THEN (correct_labels::FLOAT / tot_labels) ELSE NULL END AS rater_label_score,
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