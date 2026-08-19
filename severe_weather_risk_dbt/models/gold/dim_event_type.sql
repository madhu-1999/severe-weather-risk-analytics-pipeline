{{ config(
    materialized = 'incremental',
    unique_key = 'event_type_sk',
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
        DISTINCT event_type,
        max_loaded.max_loaded_at
    FROM
        {{ ref(
            'silver_storm_events'
        ) }}
        CROSS JOIN max_loaded

{% if is_incremental() %}
WHERE
    _loaded_at > max_loaded.max_loaded_at
{% endif %}
)
SELECT
    {{ dbt_utils.surrogate_key(['event_type']) }} AS event_type_sk,
    event_type,
    CURRENT_TIMESTAMP() AS _loaded_at
FROM
    source_data
