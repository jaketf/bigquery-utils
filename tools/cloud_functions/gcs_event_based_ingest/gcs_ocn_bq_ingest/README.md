# Event Driven BigQuery Ingest
This directory defines a reusable [Background Cloud Function](https://cloud.google.com/functions/docs/writing/background)
for ingesting any new file at a GCS prefix with a file name containing a
timestamp to be used as the partitioning and clustering column in a partitioned
BigQuery Table.

## Orchestration
1. Files pushed to a Google Cloud Storage bucket.
1. [Pub/Sub Notification](https://cloud.google.com/storage/docs/pubsub-notifications)
object finalize.
1. Cloud Function subscribes to notifications and ingests all the data into
BigQuery from a GCS prefix once a `_SUCCESS` file arrives. The success file name
is configurable with environment variable.


## Deployment
The source for this Cloud Function can easily be reused to repeat this pattern
for many tables by using the accompanying terraform module.

This way we can reuse the tested source code for the Cloud Function.

### Environment Variables
To configure each deployement of the Cloud Function we will use
[Environment Variables](https://cloud.google.com/functions/docs/env-var)
All of these environment variables are optional for overriding the
following default behavior.

| Variable              | Description                           | Default                                      |
|-----------------------|---------------------------------------|----------------------------------------------|
| `WAIT_FOR_JOB_SECONDS`| How long to wait before deciding BQ job did not fail quickly| `5` |
| `SUCCESS_FILENAME`    | Filename to trigger a load of a prefix| `_SUCCESS` |
| `DESTINATION_REGEX`   | A [Python Regex with named capturing groups](https://docs.python.org/3/howto/regex.html#non-capturing-and-named-groups) for `dataset`, `table`, (optional: `partition` or `yyyy`, `mm`, `dd`, `hh`, `batch`) | (see below)|
| `MAX_BATCH_BYTES`     | Max bytes for BigQuery Load job      | `15000000000000` ([15 TB](https://cloud.google.com/bigquery/quotas#load_jobs)|
| `JOB_PREFIX`          | Prefix for BigQuery Job IDs          | `gcf-ingest-` |
| `BQ_PROJECT`          | Default BQ project to use to submit load / query jobs | Project where Cloud Function is deployed |
| `BQ_STORAGE_PROJECT`          | Default BQ project to use for target table references if not specified in dataset capturing group | Project where Cloud Function is deployed |
| `FUNCTION_TIMEOUT_SEC`| Number of seconds set for this deployment of Cloud Function (no longer part of python38 runtime) | 60 |
| `FAIL_ON_ZERO_DML_ROWS_AFFECTED` | Treat External Queries that result in `numDmlAffectedRows = 0` as failures | True | 
| `ORDER_PER_TABLE`\*   | Force jobs to be executed sequentially (rather than parallel) based on the backlog. This is the same as having an `ORDERME` file in every config directory | `False` | 
| `START_BACKFILL_FILENAME`\*| Block submitting BigQuery Jobs for a table until this file is present at the table prefix. By default this will not happen. | `None` |
| `RESTART_BUFFER_SECONDS`\* | Buffer before Cloud Function timeout to leave before re-triggering the backfill subscriber | 30 |
| `USE_ERROR_REPORTING_API` | Should errors be reported using error reporting api to avoid cold restart (optimization) | True |

\* only affect the behavior when ordering is enabled for a table.
See [ORDERING.md](../ORDERING.md)

## Default Destination Regex
```python3
DEFAULT_DESTINATION_REGEX = (
    r"^(?P<dataset>[\w\-\._0-9]+)/"   # dataset (required)
    r"(?P<table>[\w\-_0-9]+)/?"       # table name (required)
    # break up historical v.s. incremental to separate prefixes (optional)
    r"(?:historical|incremental)?/?"
    r"(?P<partition>\$[0-9]+)?/?"     # partition decorator (optional)
    r"(?:"                            # [begin] yyyy/mm/dd/hh/ group (optional)
    r"(?P<yyyy>[0-9]{4})/?"           # partition year (yyyy) (optional)
    r"(?P<mm>[0-9]{2})?/?"            # partition month (mm) (optional)
    r"(?P<dd>[0-9]{2})?/?"            # partition day (dd)  (optional)
    r"(?P<hh>[0-9]{2})?/?"            # partition hour (hh) (optional)
    r")?"                             # [end]yyyy/mm/dd/hh/ group (optional)
    r"(?P<batch>[\w\-_0-9]+)?/"       # batch id (optional)
)
```

## Monitoring
### `monitored_tables`
In order for a table to be included in the monitoring views you must add a corresponding record to the `monitored_tables` table. This table is joined to by the `gcf_ingest_log` and is used to filter the results of that view to only the tables that you care about.
For example, our table might look like:

| table_group | load_type | source_database_name | source_dataset_name | source_table_name | target_project_name | target_dataset_name| target_table_name |
|-------------|-----------|----------------------|---------------------|-------------------|---------------------|--------------------|-------------------|
| Phase 1     | LOAD      | db                   | dataset_a           | table_a           | my-project-name     | dataset_a          | table_a           |
| Phase 1     | LOAD      | db                   | dataset_a           | table_b           | my-project-name     | dataset_a          | table_b           |

### `gcf_ingest_log`
This gives you a view of all of the attempts to load a given GCS prefix. For example, if we wanted to see the entire history of jobs for `'table_a'`, we would run the following query:
```sql
SELECT
  table_group,
  load_type,
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
  {dataset}.gcf_ingest_log
WHERE
  table_id = 'table_a'

```
This will give you something like:

| table_group   | load_type  | project_id    | dataset_id | table_id | job_type |attempted_gcs_prefix                                 |                                                     job_id                                          |     start_time      |      end_time       | query | gb_per_sec_throughput | total_slot_ms |   avg_slots_used   |  outcome  |       error_result     |
|---------------|------------|---------------|------------|----------|----------|------------------------------------------------|-----------------------------------------------------------------------------------------------------|---------------------|---------------------|-------|-----------------------|---------------|--------------------|-----------|------------------------|
|Phase 1        | LOAD       | my-project-id | dataset_a  | table_a  | LOAD     | db-dataset_a-table_a-incremental-1900-01-01-08 | gcf-ingest-db-dataset_a-table_a-incremental-1900-01-01-08-_DONE9197a23a-8791-4a0e-aab7-55b132728ca6 | 2021-01-11 18:13:52 | 2021-01-11 18:13:54 | NULL  |                   0.0 |          1132 | 0.5421455938697318 | FAILED    | {"Some error message"} |
|Phase 1        | LOAD       | my-project-id | dataset_a  | table_a  | LOAD     | db-dataset_a-table_a-incremental-1900-01-01-08 | gcf-ingest-db-dataset_a-table_a-incremental-1900-01-01-08-_DONEc0f17aa2-740f-4cae-80b7-02c185e5057a | 2021-01-11 20:04:12 | 2021-01-11 20:04:25 | NULL  |    1260021.5833333333 |         14620 | 1.1520882584712373 | SUCCEEDED |                   NULL |


### gcf_ingest_latest_by_table
This view gives you the latest status for the latest load / query job for a given table. If we wanted to see the latest job status for table 'table_a' we would run:
```sql
SELECT
  table_group,
  load_type,
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
  {dataset}.gcf_ingest_latest_by_table
WHERE
  table_id = 'table_a'

```

This will give you something like:

| table_group | load_type | project_id    | dataset_id | table_id | job_type | attempted_gcs_prefix                                | job_id                                                                                              | start_time          | end_time            | query | gb_per_sec_throughput | total_slot_ms | avg_slots_used     | outcome   | error_result |
|-------------|-----------|---------------|------------|----------|----------|------------------------------------------------|-----------------------------------------------------------------------------------------------------|---------------------|---------------------|-------|-----------------------|---------------|--------------------|-----------|--------------|
| Phase 1     | LOAD      | my-project-id | dataset_a  | table_a  | LOAD     | db-dataset_a-table_a-incremental-1900-01-01-08 | gcf-ingest-db-dataset_a-table_a-incremental-1900-01-01-08-_DONEc0f17aa2-740f-4cae-80b7-02c185e5057a | 2021-01-11 20:04:12 | 2021-01-11 20:04:25 | NULL  | 1260021.5833333333    | 14620         | 1.1520882584712373 | SUCCEEDED | NULL         |


This table is best used for a high-level view of your ingestion status.

 
## Implementation notes
1. To support notifications based on a GCS prefix
(rather than every object in the bucket), we chose to use manually
configure Pub/Sub Notifications manually and use a Pub/Sub triggered
Cloud Function.
