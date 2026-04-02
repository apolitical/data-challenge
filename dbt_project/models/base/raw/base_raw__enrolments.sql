-- Clean raw enrolments. Pass-through with standardised column names.
SELECT
    enrolment_id,
    user_id,
    course_id,
    enrolled_at,
    status
FROM {{ source('raw', 'enrolments') }}
