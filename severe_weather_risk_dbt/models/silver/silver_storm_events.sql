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
    *
FROM
    {{ ref('intermediate_staging_table') }}
    CROSS JOIN max_loaded
WHERE
    failure_reason IS NULL

{% if is_incremental() %}
AND _loaded_at > max_loaded.max_loaded_at
{% endif %}
