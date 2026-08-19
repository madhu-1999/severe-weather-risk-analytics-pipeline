{{ config(
    materialized = 'incremental',
    unique_key = ['episode_id', 'event_id'],
    incremental_strategy = 'merge',
    transient = true
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
        TRY_TO_NUMBER(
            episode_id,
            38,
            0
        ) AS episode_id,
        TRY_TO_NUMBER(
            event_id,
            38,
            0
        ) AS event_id,
        INITCAP(TRIM(state)) AS state,
        TRY_TO_NUMBER(
            state_fips,
            2,
            0
        ) AS state_fips,
        INITCAP(TRIM(event_type)) AS event_type,
        UPPER(TRIM(cz_type)) AS cz_type,
        TRY_TO_NUMBER(
            cz_fips,
            2,
            0
        ) AS cz_fips,
        INITCAP(TRIM(cz_name)) AS cz_name,
        TO_TIMESTAMP_NTZ(
            begin_datetime,
            'DD-MON-YY HH24:MI:SS'
        ) AS begin_datetime_local_ts,
        TIMEADD(
            'hour',
            -1 * CAST(REGEXP_SUBSTR(cz_timezone, '[-+]?[0-9]+') AS INT),
            TO_TIMESTAMP_NTZ(
                begin_datetime,
                'DD-MON-YY HH24:MI:SS'
            )
        ) AS begin_datetime_utc_ts,
        TO_TIMESTAMP_NTZ(
            end_date_time,
            'DD-MON-YY HH24:MI:SS'
        ) AS end_datetime_local_ts,
        TIMEADD(
            'hour',
            -1 * CAST(REGEXP_SUBSTR(cz_timezone, '[-+]?[0-9]+') AS INT),
            TO_TIMESTAMP_NTZ(
                end_date_time,
                'DD-MON-YY HH24:MI:SS'
            )
        ) AS end_datetime_utc_ts,
        cz_timezone,
        COALESCE(TRY_TO_NUMBER(injuries_direct, 38, 0), 0) AS injuries_direct,
        COALESCE(TRY_TO_NUMBER(injuries_indirect, 38, 0), 0) AS injuries_indirect,
        COALESCE(TRY_TO_NUMBER(deaths_direct, 38, 0), 0) AS deaths_direct,
        COALESCE(TRY_TO_NUMBER(deaths_indirect, 38, 0), 0) AS deaths_indirect,
        COALESCE(
            CASE
                WHEN damage_property ILIKE '%K' THEN CAST(REGEXP_REPLACE(damage_property, '[^0-9.]', '') AS FLOAT) * 1000
                WHEN damage_property ILIKE '%M' THEN CAST(REGEXP_REPLACE(damage_property, '[^0-9.]', '') AS FLOAT) * 1000000
                WHEN damage_property ILIKE '%B' THEN CAST(REGEXP_REPLACE(damage_property, '[^0-9.]', '') AS FLOAT) * 1000000000
                ELSE TRY_CAST(REGEXP_REPLACE(damage_property, '[^0-9.]', '') AS FLOAT)
            END,
            0.00
        ) AS damage_property,
        COALESCE(
            CASE
                WHEN damage_crops ILIKE '%K' THEN CAST(REGEXP_REPLACE(damage_crops, '[^0-9.]', '') AS FLOAT) * 1000
                WHEN damage_crops ILIKE '%M' THEN CAST(REGEXP_REPLACE(damage_crops, '[^0-9.]', '') AS FLOAT) * 1000000
                WHEN damage_crops ILIKE '%B' THEN CAST(REGEXP_REPLACE(damage_crops, '[^0-9.]', '') AS FLOAT) * 1000000000
                ELSE TRY_CAST(REGEXP_REPLACE(damage_crops, '[^0-9.]', '') AS FLOAT)
            END,
            0.00
        ) AS damage_crops,
        _file_name,
        _loaded_at,
        CURRENT_TIMESTAMP() AS _last_modified
    FROM
        {{ source(
            'raw_data',
            'raw_storm_events'
        ) }}
        CROSS JOIN max_loaded

{% if is_incremental() %}
WHERE
    _loaded_at > max_loaded.max_loaded_at
{% endif %}
),
states_seed AS (
    SELECT
        state,
        abbreviation
    FROM
        {{ ref('us_state_abbr') }}
),
event_type_designator_seed AS (
    SELECT
        event_name,
        designator
    FROM
        {{ ref('event_type_designator_mapping') }}
),
state_abbr_mapped AS (
    SELECT
        src.*,
        st.abbreviation AS state_abbr,
        ed.designator
    FROM
        source_data src
        LEFT JOIN states_seed st
        ON src.state = INITCAP(TRIM(st.state))
        LEFT JOIN event_type_designator_seed ed
        ON src.event_type = INITCAP(TRIM(ed.event_name))
)
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
    _last_modified,
    CASE
        WHEN episode_id IS NULL THEN 'Invalid episode_id'
        WHEN event_id IS NULL THEN 'Invalid event_id'
        WHEN state_abbr IS NULL THEN 'Invalid state'
        WHEN designator IS NULL THEN 'Invalid event_type to cz_type mapping'
        WHEN cz_type NOT IN (
            'C',
            'Z',
            'M'
        ) THEN 'Invalid cz_type'
        WHEN begin_datetime_local_ts IS NULL
        OR begin_datetime_utc_ts IS NULL THEN 'Invalid begin_datetime'
        WHEN end_datetime_local_ts IS NULL
        OR end_datetime_utc_ts IS NULL THEN 'Invalid end_datetime'
        ELSE NULL
    END AS failure_reason
FROM
    state_abbr_mapped
