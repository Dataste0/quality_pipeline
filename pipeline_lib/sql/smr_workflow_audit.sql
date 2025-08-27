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
          AND auditor_id <> ''
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

-- AUDITED JOBS TABLE
rater_correct_jobs_labels AS (
    SELECT 
        week_ending, 
        project_id, 
        workflow, 
        rater_id, 
        job_id,
        
        1::INT as is_rated,
        1::INT as is_audited,
        CASE WHEN
            ( SUM(CASE WHEN confusion_type = 'TP' THEN 1 ELSE 0 END) +
              SUM(CASE WHEN confusion_type = 'FP' THEN 1 ELSE 0 END) +
              SUM(CASE WHEN confusion_type = 'FN' THEN 1 ELSE 0 END) ) > 0 THEN 1 ELSE 0 END as is_audited_f1,

        COUNT(*) AS tot_labels,
        SUM(CASE WHEN is_correct THEN 1 ELSE 0 END) as correct_labels,
        SUM(weight) AS weighted_tot_labels,
        SUM(CASE WHEN is_correct THEN weight ELSE 0 END) AS weighted_correct_labels,

        SUM(CASE WHEN confusion_type = 'TP' THEN 1 ELSE 0 END) AS tp_count,
        SUM(CASE WHEN confusion_type = 'TN' THEN 1 ELSE 0 END) AS tn_count,
        SUM(CASE WHEN confusion_type = 'FP' THEN 1 ELSE 0 END) AS fp_count,
        SUM(CASE WHEN confusion_type = 'FN' THEN 1 ELSE 0 END) AS fn_count
    FROM alldata
    WHERE auditor_id <> ''
    GROUP BY week_ending, project_id, workflow, rater_id, job_id
),


rater_correct_jobs AS (
    SELECT 
        week_ending, 
        project_id, 
        workflow, 
        rater_id, 
        
        SUM(is_rated) as rated_jobs, 
        SUM(is_audited) as audited_jobs,
        SUM(is_audited_f1) as audited_jobs_f1,
       
        --SUM(correct_labels) as correct_labels, 
        --SUM(tot_labels) as tot_labels,
        SUM(weighted_correct_labels) as correct_labels,
        SUM(weighted_tot_labels) as tot_labels,

        SUM(tp_count) as tp_count,
        SUM(tn_count) as tn_count,
        SUM(fp_count) as fp_count,
        SUM(fn_count) as fn_count
    FROM rater_correct_jobs_labels
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
    FROM rater_correct_jobs
),
raters_above_target AS (
    SELECT
        *,
        CASE WHEN rater_score >= target_goal THEN 1 ELSE 0 END rater_is_above_target,
        CASE WHEN rater_f1score >= target_goal THEN 1 ELSE 0 END rater_is_above_target_f1
    FROM rater_score
)
,
workflow_score AS (
    SELECT 
        week_ending, 
        project_id, 
        workflow, 
        
        COUNT(rater_id)::INT as rater_count,
        SUM(rated_jobs)::INT as rated_jobs, 
        SUM(audited_jobs)::INT as audited_jobs,
        SUM(audited_jobs_f1)::INT as audited_jobs_f1,
        
        MAX(target_goal)::FLOAT as target_goal,
        SUM(rater_is_above_target)::INT as raters_above_target,
        SUM(rater_is_above_target_f1)::INT as raters_above_target_f1,

        SUM(correct_labels) AS workflow_correct_labels,
        SUM(tot_labels) AS workflow_tot_labels,
        CASE WHEN SUM(tot_labels) = 0 THEN NULL ELSE SUM(correct_labels)/SUM(tot_labels)::FLOAT END as workflow_score,
        
        AVG(rater_f1score)::FLOAT as workflow_f1score,
        AVG(rater_precision)::FLOAT as workflow_precision,
        AVG(rater_recall)::FLOAT as workflow_recall

    FROM raters_above_target
    GROUP BY week_ending, project_id, workflow
)
,

workflow_info AS (
    SELECT 
        week_ending,
        project_id,
        workflow,

        rater_count,
        auditor_count as auditor_count,
        
        rated_jobs as job_instances,
        audited_jobs as audited_instances,
        audited_jobs_f1 as audited_instances_f1,
        
        target_goal,
        raters_above_target,
        raters_above_target_f1,
        
        workflow_score,
        workflow_f1score,
        workflow_precision,
        workflow_recall,
        
        AVG(workflow_score) OVER (PARTITION BY week_ending, project_id) as project_score,
        AVG(workflow_f1score) OVER (PARTITION BY week_ending, project_id) as project_f1score,
        AVG(workflow_precision) OVER (PARTITION BY week_ending, project_id) as project_precision,
        AVG(workflow_recall) OVER (PARTITION BY week_ending, project_id) as project_recall
    FROM workflow_score W
    LEFT JOIN auditor_count A
    USING (week_ending, project_id, workflow)
)

SELECT * FROM workflow_info