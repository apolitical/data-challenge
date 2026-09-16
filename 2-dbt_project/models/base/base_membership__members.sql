-- base_membership__members.sql
-- Members as delivered by the membership platform, one row per member per source system

    SELECT
        member_id,
        full_name,
        email,
        source_system,
        signup_date,
        country
      FROM
        {{ source('raw', 'members') }}
