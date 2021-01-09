
-- This is a low-level view that finds some useful information on all of the chunks that have been processed
CREATE OR REPLACE VIEW
  `default-dataset`.gcf_ingest_log AS
SELECT
  target_project_id as project_id,
  target_dataset_name as dataset_id,
  target_table_name as table_id,
  job_type,
  -- Change this to extract the date and the chunk seperately (make this a UDF)
  -- Drop the gcf ingest and drop everything after the success file name
  REGEX_REPLACE(job_id, r'gcf-ingest-|-_DONE.*', '') AS attempted_chunk, -- attempts to exctract the name of the chunk that was processed
  job_id,
  start_time,
  end_time,
  query,
  SAFE_DIVIDE(total_bytes_processed, TIMESTAMP_DIFF(end_time, start_time, SECOND))  AS gb_per_sec_throughput,
  total_slot_ms,
  SAFE_DIVIDE(total_slot_ms, TIMESTAMP_DIFF(end_time, start_time, MILLISECOND))  AS avg_slots_used,
  (
    CASE
      WHEN info_schema.state != 'DONE' THEN info_schema.state
      WHEN info_schema.error_result IS NULL THEN 'SUCCEEDED'
    ELSE
    'FAILED'
  END
    ) AS outcome,
  error_result
FROM
  region-us.INFORMATION_SCHEMA.JOBS_BY_PROJECT info_schema
LEFT JOIN
  `default-dataset`.monitored_tables monitored_tables
   ON monitored_tables.target_table_name = info_schema.destination_table.table_id
  AND monitored_tables.target_dataset_name = info_schema.destination_table.dataset_id
LEFT JOIN
   `REPLACEME-audit-log-project-id.REPLACEME-audit-log-dataset.cloudaudit_googleapis_com_data_access` audit_log_sink 
     ON info_schema.job_id = audit_log_sink.protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobName.jobId
WHERE
   (SELECT value FROM UNNEST(labels) WHERE key = "component") = "event-based-gcs-ingest"
ORDER BY
  destination_table.project_id,
  destination_table.dataset_id,
  destination_table.table_id,
  start_time;