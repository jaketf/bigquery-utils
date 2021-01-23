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
-- This view shows the latest gcf ingestion job information for the latest ingestion job for a given table.
CREATE VIEW IF NOT EXISTS
  $DATASET.gcf_ingest_latest_by_table AS
SELECT
  table_group,
  load_type,
  project_id,
  dataset_id,
  table_id,
  job_type,
  attempted_gcs_prefix,
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
   $DATASET.gcf_ingest_log ingest_log_a
WHERE
  ingest_log_a.start_time is null
  or ingest_log_a.start_time = (
  SELECT
    MAX(start_time)
  FROM
    $DATASET.gcf_ingest_log ingest_log_b
  WHERE
    ingest_log_b.dataset_id = ingest_log_a.dataset_id
    AND ingest_log_b.table_id = ingest_log_a.table_id)
ORDER BY
  1,
  2,
  3;
