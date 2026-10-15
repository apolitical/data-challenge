-- base_engagement__events.sql
-- The authoritative event log for the recent window, one row per event

    SELECT
        event_id,
        member_id,
        event_type,
        event_timestamp
      FROM
        {{ source('raw', 'events') }}
