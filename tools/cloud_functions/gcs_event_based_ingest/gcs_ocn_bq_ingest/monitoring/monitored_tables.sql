CREATE OR REPLACE TABLE
  `default-dataset`.monitored_tables
  (

    table_group              STRING
    table_type               STRING
    load_type                STRING
    source_database_name     STRING
    source_dataset_name      STRING
    source_table_name        STRING
    target_project_name      STRING
    target_dataset_name      STRING
    target_table_name        STRING
  );