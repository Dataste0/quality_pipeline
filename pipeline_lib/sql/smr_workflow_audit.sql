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


-- Count auditors 
auditor_count AS (
    SELECT week_ending, project_id, workflow, COUNT(DISTINCT auditor_id) as auditor_count
    FROM alldata
    GROUP BY project_id, week_ending, workflow
),

-- Jobs correct
rater_jobs_correct_labels AS (
    SELECT 
        week_ending, 
        project_id, 
        workflow, 
        rater_id, 
        job_id, 
        SUM(is_correct) as correct_labels, 
        COUNT(*) as tot_labels, 
        SUM(CASE WHEN confusion_type = 'TP' THEN 1 ELSE 0 END) AS tp_count,
        SUM(CASE WHEN confusion_type = 'TN' THEN 1 ELSE 0 END) AS tn_count,
        SUM(CASE WHEN confusion_type = 'FP' THEN 1 ELSE 0 END) AS fp_count,
        SUM(CASE WHEN confusion_type = 'FN' THEN 1 ELSE 0 END) AS fn_count
    FROM alldata
    GROUP BY week_ending, project_id, workflow, rater_id, job_id
),
rater_jobs_correct AS (
    SELECT 
        week_ending, 
        project_id, 
        workflow, 
        rater_id, 
        COUNT(*) as rater_jobs, 
        SUM(correct_labels) as correct_labels, 
        SUM(tot_labels) as tot_labels,
        SUM(tp_count) as tp_count,
        SUM(tn_count) as tn_count,
        SUM(fp_count) as fp_count,
        SUM(fn_count) as fn_count
    FROM rater_jobs_correct_labels
    GROUP BY week_ending, project_id, workflow, rater_id
)
,
rater_score AS (
    SELECT
        *,
        {target}::FLOAT AS target_goal,
        CASE WHEN tot_labels = 0 THEN NULL ELSE correct_labels/tot_labels::FLOAT END AS rater_score,
        CASE WHEN tp_count+fp_count+fn_count = 0 THEN NULL ELSE (2 * tp_count)/((2 * tp_count) + fp_count + fn_count)::FLOAT END AS rater_f1score,
        CASE WHEN tp_count+fp_count = 0 THEN NULL ELSE tp_count/(tp_count + fp_count)::FLOAT END AS rater_precision,
        CASE WHEN tp_count+fn_count = 0 THEN NULL ELSE tp_count/(tp_count + fn_count)::FLOAT END AS rater_recall
    FROM rater_jobs_correct
),
raters_above_target AS (
    SELECT
        *,
        CASE WHEN rater_score >= {target}::FLOAT THEN 1 ELSE 0 END rater_is_above_target,
        CASE WHEN rater_f1score >= {target}::FLOAT THEN 1 ELSE 0 END rater_is_above_target_f1
    FROM rater_score
)
,
workflow_jobs_correct AS (
    SELECT 
        week_ending, 
        project_id, 
        workflow, 
        COUNT(*)::INT as rater_count,
        SUM(rater_jobs)::INT as rated_jobs, 
        SUM(rater_jobs)::INT as audited_jobs,
        SUM(correct_labels)::INT as correct_labels, 
        SUM(tot_labels)::INT as tot_labels,
        SUM(tp_count)::INT as tp_count,
        SUM(tn_count)::INT as tn_count,
        SUM(fp_count)::INT as fp_count,
        SUM(fn_count)::INT as fn_count,
        MAX(target_goal)::FLOAT as target_goal,
        SUM(rater_is_above_target)::INT as raters_above_target,
        SUM(rater_is_above_target_f1)::INT as raters_above_target_f1
    FROM raters_above_target
    GROUP BY week_ending, project_id, workflow
)
,

workflow_scores AS (
    SELECT
        *,
        CASE WHEN tot_labels = 0 THEN NULL ELSE correct_labels/tot_labels::FLOAT END AS workflow_score,
        CASE WHEN tp_count+fp_count+fn_count = 0 THEN NULL ELSE (2 * tp_count)/((2 * tp_count) + fp_count + fn_count)::FLOAT END AS workflow_f1score,
        CASE WHEN tp_count+fp_count = 0 THEN NULL ELSE tp_count/(tp_count + fp_count)::FLOAT END AS workflow_precision,
        CASE WHEN tp_count+fn_count = 0 THEN NULL ELSE tp_count/(tp_count + fn_count)::FLOAT END AS workflow_recall
    FROM workflow_jobs_correct
),

workflow_info AS (
    SELECT 
        W.*,
        A.auditor_count as auditor_count
    FROM workflow_scores W
    LEFT JOIN auditor_count A
    USING (week_ending, project_id, workflow)
)

SELECT * FROM workflow_info

