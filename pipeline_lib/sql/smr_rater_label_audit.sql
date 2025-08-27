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
          AND auditor_id <> ''
    ) t
    WHERE row_num = 1
)
,



-- REPORT Job Labels List
rater_correct_jobs_labels AS (
    SELECT 
        week_ending, 
        project_id, 
        workflow, 
        rater_id, 
        parent_label,

        --COUNT(job_id) as tot_labels,
        --SUM(CASE WHEN is_correct THEN 1 ELSE 0 END) as correct_labels,
        SUM(weight) AS tot_labels,
        SUM(CASE WHEN is_correct THEN weight ELSE 0 END) AS correct_labels,
        
        SUM(CASE WHEN confusion_type = 'TP' THEN 1 ELSE 0 END) AS tp_count,
        SUM(CASE WHEN confusion_type = 'TN' THEN 1 ELSE 0 END) AS tn_count,
        SUM(CASE WHEN confusion_type = 'FP' THEN 1 ELSE 0 END) AS fp_count,
        SUM(CASE WHEN confusion_type = 'FN' THEN 1 ELSE 0 END) AS fn_count
    FROM alldata
    GROUP BY week_ending, project_id, workflow, rater_id, parent_label
)
,
rater_label_score AS (
    SELECT
        *,
        {target}::FLOAT AS target_goal,
        CASE WHEN tot_labels = 0 THEN NULL ELSE correct_labels/tot_labels::FLOAT END AS rater_label_score,
        CASE WHEN tp_count+fp_count+fn_count = 0 THEN NULL ELSE (2 * tp_count)/((2 * tp_count) + fp_count + fn_count)::FLOAT END AS rater_label_f1score,
        CASE WHEN tp_count+fp_count = 0 THEN NULL ELSE tp_count/(tp_count + fp_count)::FLOAT END AS rater_label_precision,
        CASE WHEN tp_count+fn_count = 0 THEN NULL ELSE tp_count/(tp_count + fn_count)::FLOAT END AS rater_label_recall
    FROM rater_correct_jobs_labels
)
,
rater_info AS (
    SELECT 
        week_ending,
        project_id,
        workflow,
        rater_id,
        
        parent_label,   
        tot_labels,
        correct_labels,
        
        tp_count,
        tn_count,
        fp_count,
        fn_count,
        
        rater_label_score,
        rater_label_f1score,
        rater_label_precision,
        rater_label_recall,
        
        AVG(rater_label_score) OVER (PARTITION BY week_ending, project_id, workflow, rater_id) as rater_score,
        AVG(rater_label_f1score) OVER (PARTITION BY week_ending, project_id, workflow, rater_id) as rater_f1score,
        AVG(rater_label_precision) OVER (PARTITION BY week_ending, project_id, workflow, rater_id) as rater_precision,
        AVG(rater_label_recall) OVER (PARTITION BY week_ending, project_id, workflow, rater_id) as rater_recall
    FROM rater_label_score
)

SELECT * FROM rater_info