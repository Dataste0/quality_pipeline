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
rater_correct_jobs AS (
    SELECT 
        week_ending, 
        project_id, 
        workflow, 
        rater_id, 
        COUNT(*) as rated_jobs, 
        COUNT(*) as audited_jobs,
        SUM(CASE WHEN job_correct THEN 1 ELSE 0 END) as correct_jobs,
        AVG(job_score) as rater_score
    FROM alldata
    GROUP BY week_ending, project_id, workflow, rater_id
)
,
rater_score AS (
    SELECT
        *,
        {target}::FLOAT AS target_goal,
        CASE WHEN rated_jobs = 0 THEN NULL ELSE rater_score::FLOAT END AS rater_score
    FROM rater_correct_jobs
)
,
raters_above_target AS (
    SELECT
        *,
        CASE WHEN rater_score >= {target}::FLOAT THEN 1 ELSE 0 END rater_is_above_target
    FROM rater_score
)
,
workflow_jobs_correct AS (
    SELECT 
        week_ending, 
        project_id, 
        workflow, 
        COUNT(*)::INT as rater_count,
        SUM(rated_jobs)::INT as rated_jobs, 
        SUM(audited_jobs)::INT as audited_jobs,
        SUM(correct_jobs)::INT as correct_jobs, 
        SUM(rated_jobs)::INT as tot_jobs,
        AVG(rater_score)::FLOAT as score,
        0 as tp_count,
        0 as tn_count,
        0 as fp_count,
        0 as fn_count,
        MAX(target_goal)::FLOAT as target_goal,
        SUM(rater_is_above_target)::INT as raters_above_target,
        0 as raters_above_target_f1
    FROM raters_above_target
    GROUP BY week_ending, project_id, workflow
)
,
workflow_scores AS (
    SELECT
        *,
        CASE WHEN tot_jobs = 0 THEN NULL ELSE score::FLOAT END AS workflow_score,
        0::FLOAT AS workflow_f1score,
        0::FLOAT AS workflow_precision,
        0::FLOAT AS workflow_recall
    FROM workflow_jobs_correct
)
,
workflow_info AS (
    SELECT 
        W.*,
        A.auditor_count as auditor_count
    FROM workflow_scores W
    LEFT JOIN auditor_count A
    USING (week_ending, project_id, workflow)
)

SELECT
    project_id,
    week_ending,
    workflow,
    rater_count,
    auditor_count,
    rated_jobs as job_instances,
    audited_jobs as audited_instances,
    rated_jobs as label_count,
    correct_jobs as correct_label_count,
    tp_count,
    tn_count,
    fp_count,
    fn_count,
    target_goal,
    raters_above_target,
    raters_above_target_f1,
    workflow_score,
    workflow_f1score,
    workflow_precision,
    workflow_recall
FROM workflow_info

