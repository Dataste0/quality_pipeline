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
label_occurrences AS (
    SELECT
        project_id,
        job_id,
        parent_label,
        rater_response,
        COUNT(*) AS response_count,
        SUM(COUNT(*)) OVER (PARTITION BY project_id, job_id, parent_label) AS total_responses_count
    FROM
        alldata
    GROUP BY project_id, job_id, parent_label, rater_response
)
,
label_majority AS (
    SELECT 
        *,
        CASE WHEN response_count > (total_responses_count / 2) THEN 1 ELSE 0 END AS majority
    FROM label_occurrences
)
,
label_consensus AS (
    SELECT
        *,
        CASE
            WHEN total_responses_count>2 THEN 1
            WHEN total_responses_count=2 AND majority=1 THEN 1
            ELSE 0
        END AS is_there_consensus
    FROM label_majority
)
,
label_final_decision AS (
    SELECT
        *,
        CASE
            WHEN is_there_consensus = 0 THEN NULL
            WHEN is_there_consensus = 1 AND majority = 1 THEN 1
            ELSE 0
        END AS is_correct
    FROM label_consensus
)
,
-- filter correct responses (majority of contributors agree)
label_correct AS (
    SELECT 
        project_id, 
        job_id, 
        parent_label, 
        rater_response AS consensus_response
    FROM label_final_decision
    WHERE is_correct = 1
)
,
-- compare all responses with consensus responses
compare_response AS (
    SELECT 
        M.week_ending,
        M.project_id, 
        M.workflow,
        M.rater_id,
        M.job_id, 
        M.parent_label, 
        M.rater_response,
        L.consensus_response,
        M.is_label_binary as is_binary,
        M.is_positive,
        M.weight,
        -- Label is determined when consensus has been reached on the correct label (half of total reviewers + 1)
        CASE WHEN L.consensus_response IS NOT NULL THEN 1 ELSE 0 END AS is_label_determined,
        CASE
            WHEN L.consensus_response IS NULL THEN NULL
            WHEN M.rater_response = L.consensus_response THEN 1 
            ELSE 0 
        END AS is_label_correct
        
    FROM alldata M
    LEFT JOIN label_correct L USING (project_id, job_id, parent_label)
)
,
-- multireview final table
multireview_jobs_labels AS (
    SELECT 
        week_ending, 
        project_id, 
        workflow, 
        rater_id, 
        job_id, 
        parent_label, 
        rater_response, 
        consensus_response AS ground_truth_consensus, 
        is_label_determined AS has_ground_truth_consensus, 
        is_label_correct,
        is_binary,
        is_positive,
        CASE
            WHEN is_binary AND is_positive AND is_label_correct THEN 'TP'
            WHEN is_binary AND NOT is_positive AND is_label_correct THEN 'TN'
            WHEN is_binary AND is_positive AND NOT is_label_correct THEN 'FP'
            WHEN is_binary AND NOT is_positive AND NOT is_label_correct THEN 'FN'
            ELSE NULL
        END AS confusion_type
    FROM compare_response
)
,
-- considering audited any job with consensus on one label
rater_correct_jobs_labels AS (
    SELECT
        week_ending,
        project_id,
        workflow,
        rater_id,
        job_id,

        1::INT as is_rated,
        MAX(has_ground_truth_consensus) as is_audited,

        SUM(has_ground_truth_consensus) as tot_labels,
        SUM(is_label_correct) as correct_labels,
        
        SUM(CASE WHEN confusion_type = 'TP' THEN 1 ELSE 0 END) AS tp_count,
        SUM(CASE WHEN confusion_type = 'TN' THEN 1 ELSE 0 END) AS tn_count,
        SUM(CASE WHEN confusion_type = 'FP' THEN 1 ELSE 0 END) AS fp_count,
        SUM(CASE WHEN confusion_type = 'FN' THEN 1 ELSE 0 END) AS fn_count
    FROM multireview_jobs_labels
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
        SUM(correct_labels) as correct_labels,
        SUM(tot_labels) as tot_labels,
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
        CASE WHEN rater_score >= {target}::FLOAT THEN 1 ELSE 0 END rater_is_above_target,
        CASE WHEN rater_f1score >= {target}::FLOAT THEN 1 ELSE 0 END rater_is_above_target_f1
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
        0::INT as auditor_count,
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
)

SELECT * FROM workflow_info