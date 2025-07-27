,
-- REPORT
rater_label_correctness AS (
    SELECT 
        methodology,
        week_ending,
        ac_project_id,
        workflow,
        actor_id,
        parent_label,
        COUNT(*) AS label_count,
        SUM(has_ground_truth_consensus) AS determined_label_count,
        SUM(is_label_correct) AS correct_label_count,
        SUM(CASE WHEN confusion_type = 'TP' THEN 1 ELSE 0 END) AS tp_count,
        SUM(CASE WHEN confusion_type = 'TN' THEN 1 ELSE 0 END) AS tn_count,
        SUM(CASE WHEN confusion_type = 'FP' THEN 1 ELSE 0 END) AS fp_count,
        SUM(CASE WHEN confusion_type = 'FN' THEN 1 ELSE 0 END) AS fn_count
        --CASE WHEN SUM(has_ground_truth_consensus) = 0 THEN NULL ELSE SUM(is_label_correct)/SUM(has_ground_truth_consensus)::FLOAT END AS rater_score
    FROM multireview_audit_combined
    GROUP BY methodology, week_ending, ac_project_id, workflow, actor_id, parent_label
),

rater_label_score AS (
    SELECT 
        methodology,
        week_ending,
        ac_project_id,
        workflow,
        actor_id,
        parent_label,
        label_count,
        determined_label_count,
        correct_label_count,
        tp_count,
        tn_count,
        fp_count,
        fn_count,
        CASE WHEN determined_label_count = 0 THEN NULL ELSE correct_label_count/determined_label_count::FLOAT END AS rater_label_score,
        CASE WHEN tp_count+fp_count+fn_count = 0 THEN NULL ELSE (2 * tp_count)/((2 * tp_count) + fp_count + fn_count)::FLOAT END AS rater_label_f1score
    FROM rater_label_correctness
)

SELECT *
FROM rater_label_score
WHERE methodology = {methodology}