-- base_engagement__member_activity.sql
-- Upstream engagement feed snapshots, one per member per pipeline run date

    SELECT
        member_id,
        snapshot_date,
        recent_event_ids,
        legacy_event_count
      FROM
        {{ source('raw', 'member_activity') }}
