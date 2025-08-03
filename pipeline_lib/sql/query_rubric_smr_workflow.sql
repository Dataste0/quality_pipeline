
-- Dedupe
WITH rawdata AS (
    SELECT *
    FROM {input_path}
    WHERE project_id = {project_id}
    AND content_week = {content_week}
),

alldata AS (
    SELECT *, 
    'audit'::TEXT AS methodology,
    content_week AS week_ending
    FROM rawdata
),



-- Auditor count
auditor_count AS (
    SELECT methodology, week_ending, project_id, workflow, COUNT(DISTINCT auditor_id) AS auditor_count
    FROM alldata
    GROUP BY methodology, week_ending, project_id, workflow
)
,

workflow_rater_totals AS (
    SELECT 
        methodology,
        week_ending,
        project_id,
        workflow,
        rater_id,
        COUNT(*) AS job_instances,
        COUNT(*) AS audited_instances,
        SUM(1::INT) AS label_count,
        SUM(1::INT) AS determined_label_count,
        SUM(final_job_score::FLOAT) AS correct_label_count,
        0 AS tp_count,
        0 AS tn_count,
        0 AS fp_count,
        0 AS fn_count
        --CASE WHEN SUM(has_ground_truth_consensus) = 0 THEN NULL ELSE SUM(is_label_correct)/SUM(has_ground_truth_consensus)::FLOAT END AS rater_score
    FROM alldata
    GROUP BY methodology, week_ending, project_id, workflow, rater_id
),

workflow_rater_totals_target AS (
    SELECT
        *,
        {target}::FLOAT AS target_goal,
        CASE WHEN job_instances = 0 THEN NULL ELSE correct_label_count/job_instances::FLOAT END AS rater_score,
        0 AS rater_f1score
    FROM workflow_rater_totals
),

workflow_totals_above_goal AS (
    SELECT
        *,
        CASE WHEN rater_score >= {target}::FLOAT THEN 1 ELSE 0 END raters_above_target,
        0 raters_above_target_f1
    FROM workflow_rater_totals_target
),

workflow_totals AS (
    SELECT
        methodology,
        week_ending,
        project_id,
        workflow,
        COUNT(DISTINCT 'rater_id') AS rater_count,
        SUM(job_instances::INT) AS job_instances,
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
    GROUP BY methodology, week_ending, project_id, workflow
),

workflow_totals_with_auditor_count AS (
    SELECT *
    FROM workflow_totals 
    LEFT JOIN auditor_count 
    USING (methodology, week_ending, project_id, workflow)
)


SELECT * FROM workflow_totals_with_auditor_count
