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
multi_rater_label_response AS (
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
multi_rater_label_response_ct AS (
    SELECT 
        *,
        CASE
            WHEN is_binary AND is_positive AND is_label_correct THEN 'TP'
            WHEN is_binary AND NOT is_positive AND is_label_correct THEN 'TN'
            WHEN is_binary AND is_positive AND NOT is_label_correct THEN 'FP'
            WHEN is_binary AND NOT is_positive AND NOT is_label_correct THEN 'FN'
            ELSE NULL
        END AS confusion_type
    FROM multi_rater_label_response
)
,
rater_label_correctness AS (
    SELECT 
        week_ending,
        project_id,
        workflow,
        rater_id,
        parent_label,
        SUM(is_label_determined) AS label_count,
        SUM(is_label_correct) AS correct_label_count,
        SUM(CASE WHEN confusion_type = 'TP' THEN 1 ELSE 0 END) AS tp_count,
        SUM(CASE WHEN confusion_type = 'TN' THEN 1 ELSE 0 END) AS tn_count,
        SUM(CASE WHEN confusion_type = 'FP' THEN 1 ELSE 0 END) AS fp_count,
        SUM(CASE WHEN confusion_type = 'FN' THEN 1 ELSE 0 END) AS fn_count
        --CASE WHEN SUM(has_ground_truth_consensus) = 0 THEN NULL ELSE SUM(is_label_correct)/SUM(has_ground_truth_consensus)::FLOAT END AS rater_score
    FROM multi_rater_label_response_ct
    GROUP BY week_ending, project_id, workflow, rater_id, parent_label
)
,
rater_label_score AS (
    SELECT 
        week_ending,
        project_id,
        workflow,
        rater_id,
        parent_label,
        label_count,
        correct_label_count,
        tp_count,
        tn_count,
        fp_count,
        fn_count,
        CASE WHEN label_count = 0 THEN NULL ELSE correct_label_count/label_count::FLOAT END AS r_label_score,
        CASE WHEN tp_count+fp_count+fn_count = 0 THEN NULL ELSE (2 * tp_count)/((2 * tp_count) + fp_count + fn_count)::FLOAT END AS r_label_f1score,
        CASE WHEN tp_count+fp_count = 0 THEN NULL ELSE tp_count/(tp_count + fp_count)::FLOAT END AS r_label_precision,
        CASE WHEN tp_count+fn_count = 0 THEN NULL ELSE tp_count/(tp_count + fn_count)::FLOAT END AS r_label_recall
    FROM rater_label_correctness
)
,
worked_jobs_per_rater AS (
    SELECT week_ending, project_id, workflow, rater_id, COUNT(*) as rated_jobs
    FROM (
        SELECT week_ending, project_id, workflow, rater_id, job_id
        FROM alldata
        GROUP BY week_ending, project_id, workflow, rater_id, job_id
    )
    GROUP BY week_ending, project_id, workflow, rater_id
)
,

rater_details AS (
    SELECT
        week_ending,
        project_id,
        workflow,
        rater_id,
        rated_jobs::INT as rated_jobs,
        parent_label,
        label_count::INT as label_count,
        correct_label_count::INT as correct_label_count,
        tp_count::INT as tp_count,
        tn_count::INT as tn_count,
        fp_count::INT as fp_count,
        fn_count::INT as fn_count,
        r_label_score::FLOAT as r_label_score,
        r_label_f1score::FLOAT as r_label_f1score,
        r_label_precision::FLOAT as r_label_precision,
        r_label_recall::FLOAT as r_label_recall,
        SUM(label_count) OVER (PARTITION BY week_ending, project_id, workflow, rater_id, rated_jobs) AS total_label_count,
        SUM(correct_label_count) OVER (PARTITION BY week_ending, project_id, workflow, rater_id, rated_jobs) AS total_correct_label_count,

        SUM(tp_count) OVER (PARTITION BY week_ending, project_id, workflow, rater_id, rated_jobs) AS total_tp_count,
        SUM(tn_count) OVER (PARTITION BY week_ending, project_id, workflow, rater_id, rated_jobs) AS total_tn_count,
        SUM(fp_count) OVER (PARTITION BY week_ending, project_id, workflow, rater_id, rated_jobs) AS total_fp_count,
        SUM(fn_count) OVER (PARTITION BY week_ending, project_id, workflow, rater_id, rated_jobs) AS total_fn_count
        
    FROM rater_label_score S
    LEFT JOIN worked_jobs_per_rater J
    USING (week_ending, project_id, workflow, rater_id)
),

rater_weekly_info AS (
    SELECT 
        week_ending,
        project_id,
        workflow,
        rater_id,
        rated_jobs,
        parent_label,
        label_count,
        correct_label_count,
        tp_count,
        tn_count,
        fp_count,
        fn_count,
        r_label_score,
        r_label_f1score,
        r_label_precision,
        r_label_recall,
        CASE WHEN total_label_count = 0 THEN NULL ELSE total_correct_label_count/total_label_count::FLOAT END AS r_overall_score,
        CASE WHEN total_tp_count+total_fp_count+total_fn_count = 0 THEN NULL ELSE (2 * total_tp_count)/((2 * total_tp_count) + total_fp_count + total_fn_count)::FLOAT END AS r_overall_f1score,
        CASE WHEN total_tp_count+total_fp_count = 0 THEN NULL ELSE total_tp_count/(total_tp_count + total_fp_count)::FLOAT END AS r_overall_precision,
        CASE WHEN total_tp_count+total_fn_count = 0 THEN NULL ELSE total_tp_count/(total_tp_count + total_fn_count)::FLOAT END AS r_overall_recall
    FROM rater_details
)

SELECT * FROM rater_weekly_info