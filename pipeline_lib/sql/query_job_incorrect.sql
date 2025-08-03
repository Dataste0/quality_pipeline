-- REPORT

SELECT *
FROM multireview_audit_combined
WHERE is_label_correct = 0
AND has_ground_truth_consensus = 1 
AND methodology = {methodology}
LIMIT 2000