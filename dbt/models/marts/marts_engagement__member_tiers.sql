-- marts_engagement__member_tiers.sql
-- Dimension: the engagement tier the CRM holds for each member, rebuilt daily
-- Grain: 1 row per member
--
-- Rebuilt daily by the sync-member-engagement DAG, which then pushes the tier of every member in
-- this table into the CRM. Marketing automation is driven off the tier the CRM holds, so this model
-- is the input to member-facing messaging.
--
-- Scoring: a member's engagement score is the number of events the upstream feed reports for them
-- in the recent window. Members still on the legacy platform are not covered by that feed, so for
-- them the score falls back to the legacy lifetime counter.
--
-- Tiers: champion >= 50, engaged >= 20, at_risk >= 5, dormant below that.

{{ config(materialized='table') }}

WITH run_parameters AS (

    SELECT
        CAST('{{ var("run_date") }}' AS DATE) AS run_date,
        7 AS window_days

),

crm_state AS (

    -- What the CRM currently holds, ingested back into the warehouse once per run. Carries the
    -- member's previous tier through to the sync, which needs it to build the timeline note.
    SELECT
        member_id,
        engagement_tier AS previous_tier,
        is_active,
        last_synced_at
      FROM
        {{ ref('base_crm__member_state') }}
     WHERE
        state_date = (
             SELECT
                 MAX(state_date)
               FROM
                 {{ ref('base_crm__member_state') }}
              WHERE
                 state_date <= (SELECT run_date FROM run_parameters)
      )

),

recent_activity AS (

    -- The most recent upstream snapshot for each member inside the rolling window. The feed
    -- redelivers the whole population every run, so take the latest snapshot rather than summing
    -- across days.
    SELECT
        activity.member_id,
        ARG_MAX(activity.recent_event_ids, activity.snapshot_date)   AS recent_event_ids,
        ARG_MAX(activity.legacy_event_count, activity.snapshot_date) AS legacy_event_count,
        MAX(activity.snapshot_date)                                  AS latest_snapshot_date
      FROM
        {{ ref('base_engagement__member_activity') }} AS activity
INNER JOIN
        crm_state
        ON
        crm_state.member_id = activity.member_id
CROSS JOIN
        run_parameters
     WHERE
           crm_state.is_active
       AND activity.snapshot_date <= run_parameters.run_date
       AND activity.snapshot_date > run_parameters.run_date - run_parameters.window_days
  GROUP BY
        activity.member_id

),

scored AS (

    SELECT
        -- Identifiers
        members.member_id,
        members.source_system,
        members.email,
        members.country,

        -- Current CRM state
        crm_state.previous_tier,
        crm_state.last_synced_at,
        recent_activity.latest_snapshot_date,

        -- Engagement signal. Prefer the feed's own count of recent events; fall back to the legacy
        -- lifetime counter for members the feed does not cover.
        COALESCE(
            LEN(recent_activity.recent_event_ids),
            recent_activity.legacy_event_count
        ) AS engagement_score

      FROM
        {{ ref('base_membership__members') }} AS members
 LEFT JOIN
        crm_state
        ON
        crm_state.member_id = members.member_id
 LEFT JOIN
        recent_activity
        ON
        recent_activity.member_id = members.member_id

),

tiered AS (

    SELECT
        scored.member_id,
        scored.source_system,
        scored.email,
        scored.country,
        scored.previous_tier,
        scored.last_synced_at,
        scored.latest_snapshot_date,
        scored.engagement_score,
        CASE WHEN scored.engagement_score >= 50
               THEN 'champion'
             WHEN scored.engagement_score >= 20
               THEN 'engaged'
             WHEN scored.engagement_score >= 5
               THEN 'at_risk'
             ELSE 'dormant'
         END AS engagement_tier
      FROM
        scored

)

    SELECT
        -- Identifiers
        tiered.member_id,
        tiered.source_system,
        tiered.email,
        tiered.country,

        -- Engagement
        tiered.engagement_score,
        tiered.engagement_tier,
        tiered.previous_tier,
        tiered.engagement_tier <> tiered.previous_tier AS tier_changed,

        -- Provenance
        tiered.latest_snapshot_date,
        tiered.last_synced_at,
        CAST('{{ var("run_date") }}' AS DATE) AS run_date

      FROM
        tiered
