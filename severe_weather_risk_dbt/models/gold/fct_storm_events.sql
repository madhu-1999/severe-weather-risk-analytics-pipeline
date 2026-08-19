{{ config(
    materialized = 'incremental',
    unique_key = ['episode_id', 'event_id'],
    incremental_strategy = 'merge'
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
        *
    FROM
        {{ ref('silver_storm_events') }}
        CROSS JOIN max_loaded

{% if is_incremental() %}
WHERE
    _loaded_at > max_loaded.max_loaded_at
{% endif %}
),
dim_location AS (
    SELECT
        *
    FROM
        {{ ref('dim_location') }}
),
dim_event_type AS (
    SELECT
        *
    FROM
        {{ ref('dim_event_type') }}
)
SELECT
    -- Composite Natural Primary Key
    s.episode_id,
    s.event_id,
    -- Foreign Keys
    loc.location_sk,
    evt.event_type_sk,
    TO_NUMBER(to_char(s.begin_datetime_local_ts, 'YYYYMMDD')) AS begin_date_key,
    TO_NUMBER(to_char(s.end_datetime_local_ts, 'YYYYMMDD')) AS end_date_key,
    -- Exact timestamps
    s.begin_datetime_local_ts,
    s.begin_datetime_utc_ts,
    s.end_datetime_local_ts,
    s.end_datetime_utc_ts,
    -- Measures
    s.injuries_direct,
    s.injuries_indirect,
    s.deaths_direct,
    s.deaths_indirect,
    s.damage_property,
    s.damage_crops,
    s.injuries_direct + s.injuries_indirect AS total_injuries,
    s.deaths_direct + s.deaths_indirect AS total_deaths,
    s.damage_crops + s.damage_property AS total_financial_impact,
    (
        s.deaths_direct + s.deaths_indirect
    ) * 100 + (
        s.injuries_direct + s.injuries_indirect
    ) * 10 + (
        s.damage_crops + s.damage_property
    ) / 1000 AS severity_score,
    -- Metadata
    s._file_name,
    s._loaded_at
FROM
    source_data s
    LEFT JOIN dim_location loc
    ON {{ dbt_utils.surrogate_key(['s.state', 's.state_fips', 's.cz_fips', 's.cz_name', 's.cz_type', 's.cz_timezone']) }} = loc.location_sk
    LEFT JOIN dim_event_type evt
    ON {{ dbt_utils.surrogate_key(['s.event_type']) }} = evt.event_type_sk
