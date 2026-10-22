-- base_crm__member_state.sql
-- The CRM's member state, ingested back into the warehouse once per run

    SELECT
        member_id,
        state_date,
        engagement_tier,
        is_active,
        last_synced_at
      FROM
        {{ source('raw', 'crm_member_state') }}
