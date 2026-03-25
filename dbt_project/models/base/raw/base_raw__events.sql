-- Clean raw events. Renames columns to snake_case.
-- Use {{ source('raw', 'table_name') }} to reference raw tables.
SELECT
    id AS event_id,
    user_id,
    course_id,
    event_type,
    event_timestamp,
    session_id,
    metadata
FROM {{ source('raw', 'events') }}
