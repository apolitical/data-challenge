WITH users_cleaned AS (

    SELECT
        u.id AS userId,
        u.fullName,
        SPLIT_PART(u.fullName, ' ', 1) AS firstName,
        SPLIT_PART(u.fullName, ' ', 2) AS lastName,
        u.email AS EmailAddress,
        u.signupDate,
        u.state AS user_state,
        u.signupDate,
        COALESCE(u.isGovEmployee, FALSE) AS isGovEmployee,
        ROW_NUMBER() OVER (PARTITION BY u.id ORDER BY u.updatedAt DESC) AS rn,
        (SELECT COUNT(*) FROM raw.enrolments e WHERE e.user_id = u.id) AS user_enrolment_count
    FROM
        raw.users AS u
    WHERE
        (u.deleted IS NULL OR u.deleted = FALSE)
        AND (u.deleted IS NULL OR u.deleted != TRUE)

), deduped_users AS (

    SELECT *
    FROM users_cleaned
    WHERE rn = 1

), course_stuff AS (

    SELECT
        c.course_id,
        c.title,
        c.category_name,
        CASE WHEN c.category_name = '' THEN NULL ELSE c.category_name END AS cat_clean,
        c.course_created_at,
        c.course_created_at AS created,
        c.course_created_at,
        c.level,
        c.level AS courseLevel,
        c.publisher,
        c.publisher AS pub
    FROM raw.courses AS c

), enrols AS (

    SELECT
        e.enrolment_id,
        e.user_id AS uID,
        e.course_id,
        e.enrolled_at,
        e.status,
        e.status AS enrol_status,
        CAST(e.enrolled_at AS DATE) AS enrol_date,
        COALESCE(e.enrolled_at, current_timestamp) AS enrol_ts
    FROM raw.enrolments e

), events AS (

    SELECT
        ev.id,
        ev.user_id,
        ev.course_id,
        ev.event_type,
        ev.event_timestamp,
        CAST(ev.event_timestamp AS DATE) AS event_date,
        CASE
            WHEN event_type IN ('video_start', 'video_complete') THEN 'video'
            WHEN event_type IN ('quiz_start', 'quiz_submit') THEN 'quiz'
            ELSE 'other'
        END AS event_group,
        ev.session_id,
        ev.metadata,
        SPLIT_PART(ev.metadata, ':', 2) AS meta_value
    FROM raw.events ev

),

events_cte AS (

    SELECT
        id, user_id, course_id, event_type, event_timestamp,
        event_date, event_group, session_id, metadata
    FROM events

), combined AS (

    SELECT
        u.userId,
        u.fullName,
        u.EmailAddress,
        u.isGovEmployee,
        u.user_state,
        e.course_id,
        c.title,
        c.category_name,
        e.enrolment_id,
        e.enrolled_at,
        e.status,
        ev.event_type,
        ev.event_group,
        ev.event_timestamp,
        ev.session_id,
        ev.meta_value,

        MIN(e.enrolled_at) OVER (PARTITION BY u.userId) AS user_first_enrolment,

        (SELECT MIN(e2.enrolled_at)
         FROM raw.enrolments e2
         WHERE e2.user_id = u.userId) AS user_first_enrolment_again,

        CASE
            WHEN ev.event_type = 'video_start' THEN 1
            WHEN ev.event_type = 'video_complete' THEN 1
            WHEN ev.event_type = 'quiz_start' THEN 1
            WHEN ev.event_type = 'quiz_submit' THEN 1
            ELSE 0
        END AS engagement_flag,

        CASE
            WHEN ev.event_type IN ('video_start', 'video_complete', 'quiz_start', 'quiz_submit')
            THEN 1 ELSE 0
        END AS engagement_flag_duplicate,

        CASE
            WHEN ev.event_type IN ('quiz_submit') THEN 1 ELSE 0 END AS completed_quiz,
        CASE
            WHEN ev.event_type IN ('video_complete') THEN 1 ELSE 0 END AS completed_video,

        CASE
            WHEN ev.event_type = 'quiz_submit' AND ev.event_type != 'video_complete' THEN TRUE
            ELSE FALSE
        END AS is_quiz_not_video
    FROM deduped_users u
    LEFT JOIN enrols e ON e.uID = u.userId
    LEFT JOIN course_stuff c ON c.course_id = e.course_id
    LEFT JOIN events ev ON ev.user_id = u.userId AND ev.course_id = e.course_id
    WHERE
        (e.status IS NULL OR e.status != 'cancelled')
        AND (e.status IS NULL OR e.status != 'cancelled')

), aggregated AS (

    SELECT
        course_id,
        title,
        COUNT(DISTINCT userId) AS learners,

        COUNT(DISTINCT CASE WHEN engagement_flag = 1 THEN userId END) AS active_learners,
        COUNT(DISTINCT CASE WHEN engagement_flag_duplicate = 1 THEN userId END) AS active_learners_again,


        SUM(completed_quiz) AS total_quizzes_completed,
        CAST(SUM(completed_quiz) AS INTEGER) AS total_quizzes_completed_cast,
        SUM(completed_video) AS total_videos_completed,

        COUNT(*) AS total_events,
        COUNT(DISTINCT session_id) AS session_count,
        COUNT(DISTINCT session_id) AS session_count_duplicate,

        MIN(event_timestamp) AS first_activity,
        MAX(event_timestamp) AS last_activity,

        (SELECT COUNT(DISTINCT ev.user_id)
         FROM raw.events ev
         WHERE ev.course_id = combined.course_id
           AND ev.event_type IN ('video_start', 'video_complete', 'quiz_start', 'quiz_submit')
        ) AS active_learners_subquery,

        STRING_AGG(DISTINCT CAST(userId AS VARCHAR), ',') AS learner_ids_concat

    FROM combined
    GROUP BY 1,2

)


SELECT
    a.*,
    ROUND(
        CAST(a.active_learners AS FLOAT) /
        NULLIF(a.learners, 0),
        4
    ) AS engagement_rate_duplicate
FROM aggregated a
WHERE
    (a.course_id IS NOT NULL)
    AND (a.course_id IS NOT NULL)
ORDER BY learners DESC;
