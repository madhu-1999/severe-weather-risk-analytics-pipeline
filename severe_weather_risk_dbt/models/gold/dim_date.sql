{{ config(
    materialized = 'incremental',
    unique_key = 'date_key',
    incremental_strategy = 'merge',
) }}

WITH max_loaded AS (

{% if is_incremental() %}

SELECT
    COALESCE(MAX(_loaded_at), '1900-01-01' :: timestamp_ntz) AS max_loaded_at
FROM
    {{ this }}
{% else %}
SELECT
    '1900-01-01' :: timestamp_ntz AS max_loaded_at
{% endif %}),
source_data AS (
    SELECT
        begin_datetime_local_ts,
        end_datetime_local_ts,
        _loaded_at
    FROM
        {{ ref('silver_storm_events') }}
        CROSS JOIN max_loaded

{% if is_incremental() %}
WHERE
    _loaded_at > max_loaded.max_loaded_at
{% endif %}
),
-- Unpivot begin and end timestamps to capture all unique event dates
ingested_dates AS (
    SELECT
        CAST(
            begin_datetime_local_ts AS DATE
        ) AS full_date,
        _loaded_at
    FROM
        source_data
    UNION ALL
    SELECT
        CAST(
            end_datetime_local_ts AS DATE
        ) AS full_date,
        _loaded_at
    FROM
        source_data
)
SELECT
    DISTINCT TO_NUMBER(to_char(full_date, 'YYYYMMDD')) AS date_key,
    full_date,
    EXTRACT(
        YEAR
        FROM
            full_date
    ) AS YEAR,
    EXTRACT(
        MONTH
        FROM
            full_date
    ) AS MONTH,
    EXTRACT(
        DAY
        FROM
            full_date
    ) AS DAY,
    to_char(
        full_date,
        'MMMM'
    ) AS month_name,
    to_char(
        full_date,
        'DY'
    ) AS day_of_week,
    EXTRACT(
        quarter
        FROM
            full_date
    ) AS quarter,
    CURRENT_TIMESTAMP() AS _loaded_at
FROM
    ingested_dates
