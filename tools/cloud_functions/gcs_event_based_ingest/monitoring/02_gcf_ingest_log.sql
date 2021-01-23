/*
# Copyright 2020 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
*/
-- This view shows gcf ingestion job information on a per-job basis
CREATE VIEW IF NOT EXISTS
  $DATASET.gcf_ingest_log AS
SELECT
  table_group,
  load_type,
  target_project_name as project_id,
  target_dataset_name as dataset_id,
  target_table_name as table_id,
  job_type,
  REGEXP_REPLACE(job_id, r'gcf-ingest-|-_SUCCESS.*', '') AS attempted_gcs_prefix, -- attempts to extract the name of the gcs_prefix that was processed
  job_id,
  start_time,
  end_time,
  query,
  SAFE_DIVIDE(total_bytes_processed, TIMESTAMP_DIFF(end_time, start_time, SECOND))  AS gb_per_sec_throughput,
  total_slot_ms,
  SAFE_DIVIDE(total_slot_ms, TIMESTAMP_DIFF(end_time, start_time, MILLISECOND))  AS avg_slots_used,
  (
    CASE
      WHEN info_schema.state IS NULL then 'NOT RUN'
      WHEN info_schema.state != 'DONE' THEN info_schema.state
      WHEN info_schema.error_result IS NULL THEN 'SUCCEEDED'
    ELSE
    'FAILED'
  END
    ) AS outcome,
  error_result
FROM
  $DATASET.monitored_tables monitored_tables
LEFT JOIN
  `region-us.INFORMATION_SCHEMA.JOBS_BY_PROJECT` info_schema
   ON monitored_tables.target_table_name = info_schema.destination_table.table_id
  AND monitored_tables.target_dataset_name = info_schema.destination_table.dataset_id
WHERE
   (info_schema.destination_table is null
    OR (SELECT value FROM UNNEST(labels) WHERE key = "component") = "event-based-gcs-ingest")
ORDER BY
  destination_table.project_id,
  destination_table.dataset_id,
  destination_table.table_id,
  start_time;