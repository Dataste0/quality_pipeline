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

-- REPORT
label_error_count AS (
    SELECT week_ending, project_id, parent_label, rater_response, COUNT(*) AS error_count
    FROM multi_rater_label_response_ct
    WHERE is_label_determined = 1 AND is_label_correct = 0
    GROUP BY week_ending, project_id, parent_label, rater_response
),

label_error_contribution AS (
    SELECT 
        *, 
        SUM(error_count) OVER (PARTITION BY week_ending, project_id, parent_label)::INT AS label_error_count,
        error_count / SUM(error_count) OVER (PARTITION BY week_ending, project_id, parent_label)::FLOAT AS error_contribution
    FROM label_error_count
),

label_cumulative_error_contribution AS (
    SELECT 
        *,
        SUM(error_contribution) OVER (
          PARTITION BY week_ending, project_id, parent_label
          ORDER BY error_contribution DESC
          ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS cumulative_error_contribution
    FROM label_error_contribution
)


SELECT * FROM label_cumulative_error_contribution
ORDER BY week_ending, project_id, parent_label, cumulative_error_contribution ASC