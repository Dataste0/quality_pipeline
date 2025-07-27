
,
-- WORKFLOW TOTALS
workflow_rater_job_totals AS (
    SELECT 
        methodology,
        week_ending,
        ac_project_id,
        workflow,
        actor_id,
        job_id,
        CASE WHEN SUM(has_ground_truth_consensus) > 0 THEN 1 ELSE 0 END AS is_audited,
        COUNT(*) AS label_count,
        SUM(has_ground_truth_consensus) AS determined_label_count,
        SUM(is_label_correct) AS correct_label_count,
        SUM(CASE WHEN confusion_type = 'TP' THEN 1 ELSE 0 END) AS tp_count,
        SUM(CASE WHEN confusion_type = 'TN' THEN 1 ELSE 0 END) AS tn_count,
        SUM(CASE WHEN confusion_type = 'FP' THEN 1 ELSE 0 END) AS fp_count,
        SUM(CASE WHEN confusion_type = 'FN' THEN 1 ELSE 0 END) AS fn_count
        --CASE WHEN SUM(has_ground_truth_consensus) = 0 THEN NULL ELSE SUM(is_label_correct)/SUM(has_ground_truth_consensus)::FLOAT END AS rater_score
    FROM multireview_audit_combined
    GROUP BY methodology, week_ending, ac_project_id, workflow, actor_id, job_id
),

workflow_rater_totals AS (
    SELECT 
        methodology,
        week_ending,
        ac_project_id,
        workflow,
        actor_id,
        COUNT(*) AS job_instances,
        SUM(is_audited) AS audited_instances,
        SUM(label_count) AS label_count,
        SUM(determined_label_count) AS determined_label_count,
        SUM(correct_label_count) AS correct_label_count,
        SUM(tp_count) AS tp_count,
        SUM(tn_count) AS tn_count,
        SUM(fp_count) AS fp_count,
        SUM(fn_count) AS fn_count
        --CASE WHEN SUM(has_ground_truth_consensus) = 0 THEN NULL ELSE SUM(is_label_correct)/SUM(has_ground_truth_consensus)::FLOAT END AS rater_score
    FROM workflow_rater_job_totals
    GROUP BY methodology, week_ending, ac_project_id, workflow, actor_id
),

workflow_rater_totals_target AS (
    SELECT
        *,
        {target}::FLOAT AS target_goal,
        CASE WHEN determined_label_count = 0 THEN NULL ELSE correct_label_count/determined_label_count::FLOAT END AS rater_score,
        CASE WHEN tp_count+fp_count+fn_count = 0 THEN NULL ELSE (2 * tp_count)/((2 * tp_count) + fp_count + fn_count)::FLOAT END AS rater_f1score
    FROM workflow_rater_totals
),

workflow_totals_above_goal AS (
    SELECT
        *,
        CASE WHEN rater_score >= {target}::FLOAT THEN 1 ELSE 0 END raters_above_target,
        CASE WHEN rater_f1score >= {target}::FLOAT THEN 1 ELSE 0 END raters_above_target_f1
    FROM workflow_rater_totals_target
),

workflow_totals AS (
    SELECT
        methodology,
        week_ending,
        ac_project_id,
        workflow,
        COUNT(*) AS rater_count,
        SUM(job_instances) AS job_instances,
        SUM(audited_instances) AS audited_instances,
        SUM(label_count) AS label_count,
        SUM(determined_label_count) AS determined_label_count,
        SUM(correct_label_count) AS correct_label_count,
        SUM(tp_count) AS tp_count,
        SUM(tn_count) AS tn_count,
        SUM(fp_count) AS fp_count,
        SUM(fn_count) AS fn_count,
        MAX(target_goal) AS target_goal,
        SUM(raters_above_target) AS raters_above_target,
        SUM(raters_above_target_f1) AS raters_above_target_f1
    FROM workflow_totals_above_goal
    GROUP BY methodology, week_ending, ac_project_id, workflow
),

workflow_totals_with_auditor_count AS (
    SELECT *
    FROM workflow_totals 
    LEFT JOIN auditor_count 
    USING (methodology, week_ending, ac_project_id, workflow)
)


SELECT * 
FROM workflow_totals_with_auditor_count
