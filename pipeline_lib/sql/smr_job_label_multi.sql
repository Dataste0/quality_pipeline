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
        M.weight AS label_weight,
        M.rater_response,
        L.consensus_response,
        -- Label is determined when consensus has been reached on the correct label (half of total reviewers + 1)
        CASE WHEN L.consensus_response IS NOT NULL THEN 1 ELSE 0 END AS is_label_determined,
        CASE
            WHEN L.consensus_response IS NULL THEN NULL
            WHEN M.rater_response = L.consensus_response THEN 1 
            ELSE 0 
        END AS is_label_correct
        
    FROM alldata M
    LEFT JOIN label_correct L 
    USING (project_id, job_id, parent_label)
)
,
-- MULTIREVIEW FINAL TABLE
multireview_jobs_labels AS (
    SELECT 
        week_ending, 
        project_id, 
        workflow, 
        rater_id, 
        job_id, 
        parent_label, 
        label_weight,
        rater_response, 
        consensus_response AS ground_truth_consensus, 
        is_label_determined AS has_ground_truth_consensus, 
        is_label_correct
    FROM compare_response
)
,


-- REPORT
job_label_correctness AS (
    SELECT 
        week_ending,
        project_id,
        workflow,
        job_id,
        parent_label,
        SUM(CASE WHEN has_ground_truth_consensus = 1 THEN 1 ELSE 0 END) AS rater_count,
        --SUM(CASE WHEN has_ground_truth_consensus = 1 AND is_label_correct = 1 THEN 1 ELSE 0 END) AS correct_rater_count,
        --SUM(CASE WHEN has_ground_truth_consensus = 1 AND is_label_correct = 0 THEN 1 ELSE 0 END) AS incorrect_rater_count,
        SUM(CASE WHEN has_ground_truth_consensus = 1 AND is_label_correct = 1 THEN label_weight ELSE 0 END) AS correct_rater_count,
        SUM(CASE WHEN has_ground_truth_consensus = 1 AND is_label_correct = 0 THEN label_weight ELSE 0 END) AS incorrect_rater_count,

        0::INT AS tp_count,
        0::INT AS tn_count,
        0::INT AS fp_count,
        0::INT AS fn_count
    FROM multireview_jobs_labels
    GROUP BY week_ending, project_id, workflow, job_id, parent_label
)
,

job_label_score AS (
    SELECT 
        week_ending,
        project_id,
        workflow,
        job_id,
        parent_label,
        rater_count,
        correct_rater_count,
        incorrect_rater_count,
        tp_count,
        tn_count,
        fp_count,
        fn_count,
        CASE WHEN rater_count = 0 THEN NULL ELSE correct_rater_count/rater_count::FLOAT END AS job_label_score,
        CASE WHEN tp_count+fp_count+fn_count = 0 THEN NULL ELSE (2 * tp_count)/((2 * tp_count) + fp_count + fn_count)::FLOAT END AS job_label_f1score,
        CASE WHEN tp_count+fp_count = 0 THEN NULL ELSE tp_count/(tp_count + fp_count )::FLOAT END AS job_label_precision,
        CASE WHEN tp_count+fn_count = 0 THEN NULL ELSE tp_count/(tp_count + fn_count )::FLOAT END AS job_label_recall
    FROM job_label_correctness
)
,
job_score AS (
    SELECT
        *,
        AVG(job_label_score) OVER (PARTITION BY week_ending, project_id, workflow, job_id) AS job_score,
        AVG(job_label_f1score) OVER (PARTITION BY week_ending, project_id, workflow, job_id) AS job_f1score,
        AVG(job_label_precision) OVER (PARTITION BY week_ending, project_id, workflow, job_id) AS job_precision,
        AVG(job_label_recall) OVER (PARTITION BY week_ending, project_id, workflow, job_id) AS job_recall
    FROM job_label_score
)

SELECT
    week_ending,
    project_id,
    workflow,
    job_id,

    parent_label,
    rater_count,
    correct_rater_count,
    incorrect_rater_count,
    
    tp_count,
    tn_count,
    fp_count,
    fn_count,

    job_score,
    job_f1score,
    job_precision,
    job_recall

FROM job_score