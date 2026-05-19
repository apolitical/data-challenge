-- Example starter model showing how the legacy SQL could be brought into dbt.
-- Candidates are free to edit, replace, or delete this file as they refactor.

WITH users_cleaned AS (

    SELECT
        u.id AS user_id,
        u.fullName,
        u.email,
        u.signupDate,
        u.state AS user_state,
        COALESCE(u.isGovEmployee, FALSE) AS is_gov_employee,
        ROW_NUMBER() OVER (PARTITION BY u.id ORDER BY u.updatedAt DESC) AS user_version_rank
    FROM {{ source('raw', 'users') }} AS u
    WHERE COALESCE(u.deleted, FALSE) = FALSE

), users_deduped AS (

    SELECT *
    FROM users_cleaned
    WHERE user_version_rank = 1

), courses_prepped AS (

    SELECT
        c.course_id,
        c.title,
        NULLIF(c.category_name, '') AS category_name,
        c.level,
        c.publisher,
        c.course_created_at
    FROM {{ source('raw', 'courses') }} AS c

), enrolments_filtered AS (

    SELECT
        e.enrolment_id,
        e.user_id,
        e.course_id,
        e.enrolled_at,
        e.status,
        CAST(e.enrolled_at AS DATE) AS enrol_date
    FROM {{ source('raw', 'enrolments') }} AS e
    WHERE e.status IS NULL OR e.status != 'cancelled'

), events_enriched AS (

    SELECT
        ev.id AS event_id,
        ev.user_id,
        ev.course_id,
        ev.event_type,
        ev.event_timestamp,
        CAST(ev.event_timestamp AS DATE) AS event_date,
        CASE
            WHEN ev.event_type IN ('video_start', 'video_complete') THEN 'video'
            WHEN ev.event_type IN ('quiz_start', 'quiz_submit') THEN 'quiz'
            ELSE 'other'
        END AS event_group,
        ev.session_id,
        SPLIT_PART(ev.metadata, ':', 2) AS meta_value
    FROM {{ source('raw', 'events') }} AS ev

), combined AS (

    SELECT
        e.course_id,
        c.title,
        c.category_name,
        c.level,
        e.user_id,
        u.email,
        u.user_state,
        e.enrolment_id,
        e.enrolled_at,
        ev.event_type,
        ev.event_group,
        ev.event_timestamp,
        ev.session_id,
        CASE
            WHEN ev.event_type IN ('video_start', 'video_complete', 'quiz_submit')
            THEN 1 ELSE 0
        END AS engagement_flag,
        CASE WHEN ev.event_type = 'quiz_submit' THEN 1 ELSE 0 END AS completed_quiz,
        CASE WHEN ev.event_type = 'video_complete' THEN 1 ELSE 0 END AS completed_video,
        MIN(e.enrolled_at) OVER (PARTITION BY e.user_id) AS user_first_enrolment,
        (
            SELECT COUNT(*)
            FROM {{ source('raw', 'enrolments') }} AS e2
            WHERE e2.user_id = e.user_id
        ) AS enrolment_count
    FROM enrolments_filtered AS e
    LEFT JOIN users_deduped AS u
        ON u.user_id = e.user_id
    LEFT JOIN courses_prepped AS c
        ON c.course_id = e.course_id
    LEFT JOIN events_enriched AS ev
        ON ev.user_id = e.user_id
       AND ev.course_id = e.course_id

), aggregated AS (

    SELECT
        course_id,
        title,
        category_name,
        level,
        COUNT(DISTINCT user_id) AS learners,
        COUNT(DISTINCT CASE WHEN engagement_flag = 1 THEN user_id END) AS active_learners,
        SUM(completed_quiz) AS total_quizzes_completed,
        SUM(completed_video) AS total_videos_completed,
        COUNT(*) AS joined_rows,
        COUNT(DISTINCT session_id) AS session_count,
        MIN(event_timestamp) AS first_activity,
        MAX(event_timestamp) AS last_activity
    FROM combined
    GROUP BY 1, 2, 3, 4

)

SELECT
    a.*,
    ROUND(
        CAST(a.active_learners AS FLOAT) /
        NULLIF(a.learners, 0),
        4
    ) AS engagement_rate
FROM aggregated AS a
WHERE a.course_id IS NOT NULL
  AND a.first_activity IS NOT NULL -- exclude courses with no activity
ORDER BY learners DESC, course_id
