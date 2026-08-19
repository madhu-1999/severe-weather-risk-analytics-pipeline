{{ config(
    materialized = 'incremental',
    unique_key = ['episode_id', 'event_id'],
    incremental_strategy = 'merge',
    transient = False
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
{% endif %})
SELECT
    episode_id,
    event_id,
    state,
    state_abbr,
    state_fips,
    event_type,
    cz_type,
    cz_fips,
    cz_name,
    begin_datetime_local_ts,
    begin_datetime_utc_ts,
    end_datetime_local_ts,
    end_datetime_utc_ts,
    cz_timezone,
    injuries_direct,
    injuries_indirect,
    deaths_direct,
    deaths_indirect,
    damage_property,
    damage_crops,
    _file_name,
    _loaded_at,
    _last_modified
FROM
    {{ ref('intermediate_staging_table') }}
    CROSS JOIN max_loaded
WHERE
    failure_reason IS NOT NULL

{% if is_incremental() %}
AND _loaded_at > max_loaded.max_loaded_at
{% endif %}
