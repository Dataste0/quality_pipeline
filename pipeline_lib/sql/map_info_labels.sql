
-- MAP LABELS

SELECT project_id, parent_label
FROM {input_path}
GROUP BY project_id, parent_label
