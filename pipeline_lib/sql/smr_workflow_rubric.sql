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

-- Count auditors 
auditor_count AS (
    SELECT week_ending, project_id, workflow, COUNT(DISTINCT auditor_id) as auditor_count
    FROM alldata
    GROUP BY project_id, week_ending, workflow
)
,
rater_correct_jobs_labels AS (
    SELECT 
        week_ending, 
        project_id, 
        workflow, 
        rater_id,
        job_id,
        1::INT as is_rated,
        MAX(CASE WHEN job_correct IS NOT NULL THEN 1 ELSE 0 END) as is_audited,
        MAX(CASE WHEN job_correct THEN 1 ELSE 0 END) as is_correct,
        MAX(CASE WHEN job_score IS NOT NULL THEN job_score ELSE NULL END) as job_score
    FROM alldata
    GROUP BY week_ending, project_id, workflow, rater_id, job_id
)
,
rater_correct_jobs AS (
    SELECT 
        week_ending, 
        project_id, 
        workflow, 
        rater_id, 
        SUM(is_rated) as rated_jobs, 
        SUM(is_audited) as audited_jobs,
        SUM(is_correct) as correct_labels, 
        COUNT(*) as tot_labels,
        AVG(job_score) as rater_score,
        0::INT as tp_count,
        0::INT as tn_count,
        0::INT as fp_count,
        0::INT as fn_count
    FROM rater_correct_jobs_labels
    GROUP BY week_ending, project_id, workflow, rater_id
)
,
rater_score AS (
    SELECT
        *,
        {target}::FLOAT AS target_goal,
        0::FLOAT AS rater_f1score,
        0::FLOAT AS rater_precision,
        0::FLOAT AS rater_recall
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
        MAX(target_goal)::FLOAT as target_goal,
        SUM(rater_is_above_target)::INT as raters_above_target,
        SUM(rater_is_above_target_f1)::INT as raters_above_target_f1,
        AVG(rater_score)::FLOAT as workflow_score,
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