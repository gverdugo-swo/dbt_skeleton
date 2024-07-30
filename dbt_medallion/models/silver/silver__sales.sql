{{ config(
    tags = ['full-materialization','sales','auto-generated']
) }}

WITH cleaning_view AS ({{ get_cleaning_query("sales") }}),
fields_view AS ({{ get_fields_query("sales") }}),
FINAL AS(
    SELECT
        *
    FROM
        fields_view
)
SELECT
    *
FROM
    FINAL
