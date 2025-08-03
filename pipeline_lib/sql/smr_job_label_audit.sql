-- Dedupe
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
job_label_correctness AS (
    SELECT 
        week_ending,
        project_id,
        workflow,
        job_id,
        parent_label,
        COUNT(*) AS rater_count,
        SUM(is_correct) AS correct_rater_count,
        SUM(CASE WHEN confusion_type = 'TP' THEN 1 ELSE 0 END) AS tp_count,
        SUM(CASE WHEN confusion_type = 'TN' THEN 1 ELSE 0 END) AS tn_count,
        SUM(CASE WHEN confusion_type = 'FP' THEN 1 ELSE 0 END) AS fp_count,
        SUM(CASE WHEN confusion_type = 'FN' THEN 1 ELSE 0 END) AS fn_count
    FROM alldata
    GROUP BY week_ending, project_id, workflow, job_id, parent_label
),

job_label_score AS (
    SELECT 
        week_ending,
        project_id,
        workflow,
        job_id,
        parent_label,
        rater_count,
        correct_rater_count,
        tp_count,
        tn_count,
        fp_count,
        fn_count,
        CASE WHEN rater_count = 0 THEN NULL ELSE correct_rater_count/rater_count::FLOAT END AS job_label_score,
        CASE WHEN tp_count+fp_count+fn_count = 0 THEN NULL ELSE (2 * tp_count)/((2 * tp_count) + fp_count + fn_count)::FLOAT END AS job_label_f1score,
        CASE WHEN tp_count+fp_count = 0 THEN NULL ELSE tp_count/(tp_count + fp_count )::FLOAT END AS job_label_precision,
        CASE WHEN tp_count+fn_count = 0 THEN NULL ELSE tp_count/(tp_count + fn_count )::FLOAT END AS job_label_recall
    FROM job_label_correctness
)

SELECT *
FROM job_label_score