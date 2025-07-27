,
-- REPORT
label_error_count AS (
    SELECT methodology, week_ending, ac_project_id, parent_label, ground_truth_consensus, response_data, COUNT(*) AS error_count
    FROM multireview_audit_combined
    WHERE ground_truth_consensus <> response_data
    GROUP BY methodology, week_ending, ac_project_id, parent_label, ground_truth_consensus, response_data
),

label_error_contribution AS (
    SELECT 
        *, 
        SUM(error_count) OVER (PARTITION BY methodology, week_ending, ac_project_id, parent_label) AS label_error_count,
        error_count / SUM(error_count) OVER (PARTITION BY methodology, week_ending, ac_project_id, parent_label)::FLOAT AS error_contribution
    FROM label_error_count
),

label_cumulative_error_contribution AS (
    SELECT 
        *,
        SUM(error_contribution) OVER (
          PARTITION BY methodology, week_ending, ac_project_id, parent_label
          ORDER BY error_contribution DESC
          ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS cumulative_error_contribution
    FROM label_error_contribution
)


SELECT * FROM label_cumulative_error_contribution
WHERE methodology = {methodology}
ORDER BY methodology, week_ending, ac_project_id, parent_label, cumulative_error_contribution ASC