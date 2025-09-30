-- VIEW TABLE_COMPARISON_SAMPLES (ARUNN_ADMIN)

  CREATE OR REPLACE FORCE EDITIONABLE VIEW "ARUNN_ADMIN"."TABLE_COMPARISON_SAMPLES" ("TABLE_NAME", "COLUMN_NAME", "MISMATCH_COUNT", "QUERY", "SAMPLE_VALUE_A", "SAMPLE_VALUE_B") AS 
  WITH SampleData AS (
    -- This CTE assigns a row number to each record within a group of the same table and column name.
    SELECT
        table_name,
        column_name,
        value_a,
        value_b,
        ROW_NUMBER() OVER(PARTITION BY table_name, column_name ORDER BY common_column_val) as rn
    FROM
        post_mig_mis_match_cols
)
SELECT
    tab.table_name,
    tab.column_name,
    tab.mismatch_count,
    tab.query_string AS query,
    s.value_a AS sample_value_a,
    s.value_b AS sample_value_b
FROM
    post_mig_mis_match_tab tab
JOIN
    SampleData s ON tab.table_name = s.table_name AND tab.column_name = s.column_name
WHERE
    s.rn = 1
;
