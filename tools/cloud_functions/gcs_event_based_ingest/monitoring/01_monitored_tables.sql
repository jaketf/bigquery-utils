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
CREATE TABLE
    $DATASET.monitored_tables
  (
    table_group              STRING, -- This field is used to group tables together E.G 'Phase 1'
    load_type                STRING, -- The expected job_type for this table. e.g 'LOAD' or 'QUERY'
    source_database_name     STRING,
    source_dataset_name      STRING,
    source_table_name        STRING,
    target_project_name      STRING,
    target_dataset_name      STRING,
    target_table_name        STRING
  );
