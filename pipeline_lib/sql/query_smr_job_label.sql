,
-- REPORT
job_label_correctness AS (
    SELECT 
        methodology,
        week_ending,
        ac_project_id,
        workflow,
        job_id,
        parent_label,
        COUNT(*) AS rater_count,
        CASE WHEN MAX(has_ground_truth_consensus) = 1 THEN TRUE ELSE FALSE END AS is_determined_label,
        SUM(is_label_correct) AS correct_rater_count,
        SUM(CASE WHEN confusion_type = 'TP' THEN 1 ELSE 0 END) AS tp_count,
        SUM(CASE WHEN confusion_type = 'TN' THEN 1 ELSE 0 END) AS tn_count,
        SUM(CASE WHEN confusion_type = 'FP' THEN 1 ELSE 0 END) AS fp_count,
        SUM(CASE WHEN confusion_type = 'FN' THEN 1 ELSE 0 END) AS fn_count
    FROM multireview_audit_combined
    GROUP BY methodology, week_ending, ac_project_id, workflow, job_id, parent_label
),

job_label_score AS (
    SELECT 
        methodology,
        week_ending,
        ac_project_id,
        workflow,
        job_id,
        parent_label,
        rater_count,
        is_determined_label,
        correct_rater_count,
        tp_count,
        tn_count,
        fp_count,
        fn_count,
        CASE WHEN is_determined_label = 0 THEN NULL ELSE correct_rater_count/rater_count::FLOAT END AS job_label_score,
        CASE WHEN tp_count+fp_count+fn_count = 0 THEN NULL ELSE (2 * tp_count)/((2 * tp_count) + fp_count + fn_count)::FLOAT END AS job_label_f1score
    FROM job_label_correctness
)

SELECT *
FROM job_label_score
WHERE methodology = {methodology}