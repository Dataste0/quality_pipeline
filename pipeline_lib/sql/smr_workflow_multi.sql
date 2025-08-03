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
        M.is_binary,
        M.is_positive,
        M.weight,
        -- Label is determined when consensus has been reached on the correct label (half of total reviewers + 1)
        CASE
            WHEN L.consensus_response IS NOT NULL THEN 1
            ELSE 0 
        END AS is_label_determined,

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
multireview_jobs_table AS (
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
        is_label_correct
    FROM compare_response
)
,
-- considering audited any job with consensus on one label
rater_jobs_correct_labels AS (
    SELECT
        week_ending,
        project_id,
        workflow,
        rater_id,
        job_id,
        COUNT(*) as total_labels,
        SUM(is_label_correct) as correct_labels,
        SUM(has_ground_truth_consensus) as consensus_labels,
        MAX(has_ground_truth_consensus) as job_is_audited,

    FROM multireview_jobs_table
    GROUP BY week_ending, project_id, workflow, rater_id, job_id
)
,
rater_correct_jobs AS (
    SELECT
        week_ending,
        project_id,
        --workflow,
        rater_id,
        COUNT(*) as rated_jobs,
        SUM(correct_labels) as correct_labels,
        SUM(consensus_labels) as total_labels,
        SUM(job_is_audited) as audited_jobs,

    FROM rater_jobs_correct_labels
    GROUP BY week_ending, project_id, workflow, rater_id
)

SELECT * FROM alldata


