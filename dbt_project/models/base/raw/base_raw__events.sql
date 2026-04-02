-- Clean raw events. Renames columns to snake_case.
SELECT
    id AS event_id,
    user_id,
    course_id,
    event_type,
    event_timestamp,
    session_id,
    metadata
FROM {{ source('raw', 'events') }}
