-- Continuous date spine: one row per calendar date, no gaps.
-- Spans from the earliest to latest event date.
-- Use {{ ref('base_calendar__dates') }} to reference this in downstream models.
SELECT d::DATE AS calendar_date
FROM generate_series(
    (SELECT MIN(event_timestamp::DATE) FROM {{ source('raw', 'events') }}),
    (SELECT MAX(event_timestamp::DATE) FROM {{ source('raw', 'events') }}),
    INTERVAL '1 day'
) AS t(d)
