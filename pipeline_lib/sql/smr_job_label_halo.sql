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
job_label_correctness AS (
    SELECT 
        week_ending,
        project_id,
        workflow,
        job_id,
        'default_label' as parent_label,
        
        COUNT(*) as total_rater_count,
        SUM(CASE WHEN job_correct IS TRUE THEN 1 ELSE 0 END) AS correct_rater_count,
        SUM(CASE WHEN job_correct IS FALSE THEN 1 ELSE 0 END) AS incorrect_rater_count,
        
        0::INT AS tp_count,
        0::INT AS tn_count,
        0::INT AS fp_count,
        0::INT AS fn_count
    FROM alldata
    GROUP BY week_ending, project_id, workflow, job_id
),

job_label_score AS (
    SELECT 
        week_ending,
        project_id,
        workflow,
        job_id,
        parent_label,
        
        total_rater_count AS rater_count,
        correct_rater_count,
        incorrect_rater_count,
        
        tp_count,
        tn_count,
        fp_count,
        fn_count,
        
        CASE WHEN total_rater_count > 0 THEN (correct_rater_count::FLOAT / total_rater_count) ELSE NULL END AS job_label_score,
        0::FLOAT AS job_label_f1score,
        0::FLOAT AS job_label_precision,
        0::FLOAT AS job_label_recall
    FROM job_label_correctness
),

job_score AS (
    SELECT
        week_ending,
        project_id,
        workflow,
        job_id,

        parent_label,
        rater_count,
        correct_rater_count,
        incorrect_rater_count,
        
        tp_count,
        tn_count,
        fp_count,
        fn_count,
        
        AVG(job_label_score) OVER (PARTITION BY week_ending, project_id, workflow, job_id) AS job_score,
        0::FLOAT AS job_f1score,
        0::FLOAT AS job_precision,
        0::FLOAT AS job_recall
    FROM job_label_score
)

SELECT
    week_ending,
    project_id,
    workflow,
    job_id,

    parent_label,
    rater_count,
    correct_rater_count,
    incorrect_rater_count,
    
    tp_count,
    tn_count,
    fp_count,
    fn_count,

    job_score,
    job_f1score,
    job_precision,
    job_recall

FROM job_score