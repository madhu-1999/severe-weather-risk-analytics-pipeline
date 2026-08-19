{{ config(
    materialized = 'incremental',
    unique_key = 'location_sk',
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
        DISTINCT state,
        state_abbr,
        state_fips,
        cz_type,
        cz_fips,
        cz_name,
        cz_timezone
    FROM
        {{ ref('silver_storm_events') }}
        CROSS JOIN max_loaded

{% if is_incremental() %}
WHERE
    _loaded_at > max_loaded.max_loaded_at
{% endif %}
)
SELECT
    {{ dbt_utils.surrogate_key(['state', 'state_fips', 'cz_fips', 'cz_name', 'cz_type', 'cz_timezone']) }} AS location_sk,
    state,
    state_abbr,
    state_fips,
    cz_type,
    cz_fips,
    cz_name,
    cz_timezone,
    CURRENT_TIMESTAMP() AS _loaded_at
FROM
    source_data dbt_utils
