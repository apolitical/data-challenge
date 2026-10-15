-- assert_dormant_share_is_stable.sql
-- The share of members in the dormant tier should not move sharply between runs
--
-- Dormancy is a slow-moving property: members drift in and out of it a few dozen at a time. A
-- sudden jump means either a genuine change in member behaviour or, far more likely, that
-- something upstream of the tier calculation has changed. Either way the run should stop before
-- the tiers reach the CRM, because marketing automation fires on the transition into dormant and a
-- re-engagement email cannot be un-sent.
--
-- Baseline is what the CRM currently holds, so the check needs no hardcoded constant. Fails when
-- the dormant share grows by more than half again over the CRM's share.

WITH current_crm AS (

    SELECT
        COUNT(*)                                                 AS members,
        COUNT(*) FILTER (WHERE engagement_tier = 'dormant')      AS dormant
      FROM
        {{ ref('base_crm__member_state') }}
     WHERE
        state_date = (
             SELECT
                 MAX(state_date)
               FROM
                 {{ ref('base_crm__member_state') }}
              WHERE
                 state_date <= CAST('{{ var("run_date") }}' AS DATE)
      )

),

this_run AS (

    SELECT
        COUNT(*)                                                 AS members,
        COUNT(*) FILTER (WHERE engagement_tier = 'dormant')      AS dormant
      FROM
        {{ ref('marts_engagement__member_tiers') }}

)

    SELECT
        current_crm.dormant::DOUBLE / current_crm.members AS crm_dormant_share,
        this_run.dormant::DOUBLE / this_run.members        AS run_dormant_share
      FROM
        current_crm
CROSS JOIN
        this_run
     WHERE
        this_run.dormant::DOUBLE / this_run.members
            > 1.5 * (current_crm.dormant::DOUBLE / current_crm.members)
