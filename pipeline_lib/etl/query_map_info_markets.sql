
-- MAP MARKET

SELECT project_id, workflow
FROM {input_path}
GROUP BY project_id, workflow
