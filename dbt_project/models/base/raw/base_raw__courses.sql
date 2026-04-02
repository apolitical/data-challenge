-- Clean raw courses. Pass-through with standardised column names.
SELECT
    course_id,
    title,
    category_name,
    level,
    publisher,
    course_created_at
FROM {{ source('raw', 'courses') }}
