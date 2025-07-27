-- Dedupe
WITH rawdata AS (
    SELECT *
    FROM (
        SELECT *, 
               ROW_NUMBER() OVER (
                   PARTITION BY project_id, job_id, actor_id, parent_label, is_audit, source_of_truth
               ) AS row_num
        FROM {input_path}
        WHERE parent_label IS NOT NULL 
          AND parent_label <> '' 
          AND parent_label <> 'pipeline_error'
          AND project_id = {project_id}
          AND content_week = {content_week}
    ) t
    WHERE row_num = 1
),

alldata AS (
    SELECT 
        *, 
        content_week AS week_ending,
        project_id AS ac_project_id
    FROM rawdata
),


-- AUDITS
alldata_audit_jobs AS (
    SELECT week_ending, ac_project_id, workflow, actor_id, a.job_id, parent_label, response_data, recall_precision AS confusion_type, 'audit'::TEXT AS methodology, is_audit, source_of_truth
    FROM alldata a
    INNER JOIN (
        SELECT DISTINCT job_id
        FROM alldata
        WHERE is_audit = '1' OR source_of_truth = '1'
    ) t
    ON a.job_id = t.job_id
),

ground_truths AS (
    SELECT
        ac_project_id,
        job_id,
        parent_label,
        response_data
    FROM alldata_audit_jobs
    WHERE is_audit = '1' OR source_of_truth = '1'
    GROUP BY ac_project_id, job_id, parent_label, response_data
),

rater_answers AS (
    SELECT *
    FROM alldata_audit_jobs
    WHERE response_data IS NOT NULL
    AND (is_audit = '0' OR is_audit IS NULL OR is_audit = '') 
    AND (source_of_truth = '0' OR source_of_truth IS NULL OR source_of_truth = '')
),
rater_answers_ground_truths AS (
    SELECT 
        R.methodology,
        R.week_ending,
        R.ac_project_id,
        R.workflow,
        R.actor_id,
        R.job_id,
        R.parent_label,
        R.response_data,
        G.response_data AS ground_truth,
        R.confusion_type,
        CASE WHEN G.response_data IS NOT NULL THEN 1 ELSE 0 END AS has_ground_truth,
        --CASE WHEN G.response_data IS NULL THEN 1 ELSE 0 END AS has_not_ground_truth,
        CASE 
            WHEN G.response_data IS NULL THEN NULL
            WHEN R.response_data = G.response_data THEN 1 ELSE 0 
        END AS is_label_correct
        --CASE WHEN R.response_data <> G.response_data THEN 1 ELSE 0 END AS is_incorrect
    FROM rater_answers R
    LEFT JOIN ground_truths G
    USING (ac_project_id, job_id, parent_label)
),
-- audits final table
audit_jobs_table AS (
    SELECT 
        methodology, 
        week_ending, 
        ac_project_id, 
        workflow, 
        actor_id, 
        job_id, 
        parent_label, 
        response_data, 
        ground_truth AS ground_truth_consensus, 
        confusion_type, 
        has_ground_truth AS has_ground_truth_consensus, 
        is_label_correct
    FROM rater_answers_ground_truths R
),


-- MULTI-REVIEW
alldata_multireview_jobs AS (
    SELECT week_ending, ac_project_id, workflow, actor_id, a.job_id, parent_label, response_data, recall_precision AS confusion_type, 'multi'::TEXT AS methodology
    FROM alldata a
    -- excluding audited job_ids from the list
    LEFT JOIN (
        SELECT DISTINCT job_id
        FROM alldata
        WHERE is_audit = '1' OR source_of_truth = '1'
    ) excluded
    ON a.job_id = excluded.job_id
    WHERE excluded.job_id IS NULL
),

label_occurrences AS (
    SELECT
        ac_project_id,
        job_id,
        parent_label,
        response_data,
        COUNT(*) AS response_count,
        SUM(COUNT(*)) OVER (PARTITION BY ac_project_id, job_id, parent_label) AS total_responses_count
    FROM
        alldata_multireview_jobs
    GROUP BY ac_project_id, job_id, parent_label, response_data
),
-- check if response has majority
label_majority AS (
    SELECT 
        *,
        CASE WHEN response_count > (total_responses_count / 2) THEN 1 ELSE 0 END AS majority
    FROM label_occurrences
),
-- detect if there's enough answers for a consensus on the response
label_consensus AS (
    SELECT
        *,
        CASE
            WHEN total_responses_count>2 THEN 1
            WHEN total_responses_count=2 AND majority=1 THEN 1
            ELSE 0
        END AS is_there_consensus
    FROM label_majority
),
-- detect if the answer is considered correct based on the agreement of more contributors
label_final_decision AS (
    SELECT
        *,
        CASE
            WHEN is_there_consensus = 0 THEN NULL
            WHEN is_there_consensus = 1 AND majority = 1 THEN 1
            ELSE 0
        END AS is_correct
    FROM label_consensus
),
-- filter correct responses (majority of contributors agree)
label_correct AS (
    SELECT 
        ac_project_id, 
        job_id, 
        parent_label, 
        response_data AS consensus_response
    FROM label_final_decision
    WHERE is_correct = 1
),
-- compare all responses with consensus responses
compare_response AS (
    SELECT 
        M.methodology,
        M.week_ending,
        M.ac_project_id, 
        M.workflow,
        M.actor_id,
        M.job_id, 
        M.parent_label, 
        M.response_data, 
        L.consensus_response,
        M.confusion_type,
        -- Label is determined when consensus has been reached on the correct label (half of total reviewers + 1)
        CASE
            WHEN L.consensus_response IS NOT NULL THEN 1
            ELSE 0 
        END AS is_label_determined,
        -- Label is undetermined when consensus has NOT been reached on the correct label (means labeled by only one reviewer or 2 reviewers in disagreement)
        --CASE
        --    WHEN L.consensus_response IS NULL THEN 1
        --    ELSE 0 
        --END AS is_label_undetermined,
        CASE
            WHEN L.consensus_response IS NULL THEN NULL
            WHEN M.response_data = L.consensus_response THEN 1 
            ELSE 0 
        END AS is_label_correct
        --CASE
        --    WHEN L.consensus_response IS NULL THEN NULL
        --    WHEN M.response_data <> L.consensus_response THEN 1 
        --    ELSE 0 
        --END AS is_label_incorrect
    FROM alldata_multireview_jobs M
    LEFT JOIN label_correct L USING (ac_project_id, job_id, parent_label)
),
-- multireview final table
multireview_jobs_table AS (
    SELECT 
        methodology, 
        week_ending, 
        ac_project_id, 
        workflow, 
        actor_id, 
        job_id, 
        parent_label, 
        response_data, 
        consensus_response AS ground_truth_consensus, 
        confusion_type, 
        is_label_determined AS has_ground_truth_consensus, 
        is_label_correct
    FROM compare_response C
),



-- Multireview and Audits combined - extended table
multireview_audit_combined AS (
    SELECT * FROM multireview_jobs_table
    UNION ALL
    SELECT * FROM audit_jobs_table
),


-- Auditor count
auditor_count AS (
    SELECT methodology, week_ending, ac_project_id, workflow, COUNT(DISTINCT actor_id) AS auditor_count
    FROM alldata_audit_jobs
    WHERE is_audit = '1' OR source_of_truth = '1'
    GROUP BY methodology, week_ending, ac_project_id, workflow
)