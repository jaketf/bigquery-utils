CREATE OR REPLACE VIEW
  `default-dataset`.gcf_ingest_latest_by_table AS
SELECT
  project_id,
  dataset_id,
  table_id,
  job_type,
  attempted_chunk,
  job_id,
  start_time,
  end_time,
  query,
  gb_per_sec_throughput,
  total_slot_ms,
  avg_slots_used,
  outcome,
  error_result
FROM
   `default-dataset`.gcf_ingest_log ingest_log_a
WHERE
  ingest_log_a.start_time = (
  SELECT
    MAX(start_time)
  FROM
    `default-dataset`.gcf_ingest_log ingest_log_b
  WHERE
    ingest_log_b.dataset_id = ingest_log_a.dataset_id
    AND ingest_log_b.table_id = ingest_log_a.table_id
    AND ingest_log_b.attempted_chunk = ingest_log_a.attempted_chunk )
ORDER BY
  1,
  2,
  3;