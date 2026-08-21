-- Every (episode_id, event_id) that lands in intermediate_staging_table must end up
-- in exactly one of silver_storm_events or silver_storm_events_quarantine.
-- Guards against rows silently disappearing due to a join/filter bug, and against
-- the same row appearing in both downstream tables.
WITH staged AS (
    SELECT
        DISTINCT episode_id,
        event_id
    FROM
        {{ ref('intermediate_staging_table') }}
),
good AS (
    SELECT
        DISTINCT episode_id,
        event_id
    FROM
        {{ ref('silver_storm_events') }}
),
quarantined AS (
    SELECT
        DISTINCT episode_id,
        event_id
    FROM
        {{ ref('silver_storm_events_quarantine') }}
),
routed AS (
    SELECT
        *
    FROM
        good
    UNION ALL
    SELECT
        *
    FROM
        quarantined
),
routed_dedup AS (
    SELECT
        episode_id,
        event_id,
        COUNT(*) AS route_count
    FROM
        routed
    GROUP BY
        1,
        2
) -- fails if: a staged row is missing from both downstream tables (route_count IS NULL),
-- or a row incorrectly landed in both (route_count > 1)
SELECT
    s.episode_id,
    s.event_id,
    COALESCE(
        r.route_count,
        0
    ) AS route_count
FROM
    staged s
    LEFT JOIN routed_dedup r
    ON s.episode_id = r.episode_id
    AND s.event_id = r.event_id
WHERE
    COALESCE(
        r.route_count,
        0
    ) <> 1
