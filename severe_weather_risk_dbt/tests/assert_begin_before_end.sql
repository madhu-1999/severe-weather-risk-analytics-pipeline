-- An event cannot end before it begins. Returns offending rows (test fails if any are returned).
SELECT
    episode_id,
    event_id,
    begin_datetime_local_ts,
    end_datetime_local_ts
FROM
    {{ ref('silver_storm_events') }}
WHERE
    begin_datetime_local_ts > end_datetime_local_ts
