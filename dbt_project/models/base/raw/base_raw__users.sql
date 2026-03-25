-- Deduplicate users by id, keeping the most recent record.
-- Filters out deleted users.
-- Renames columns to snake_case.
WITH ranked AS (
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY id ORDER BY updatedAt DESC) AS rn
    FROM {{ source('raw', 'users') }}
    WHERE deleted IS NULL OR deleted = FALSE
)
SELECT
    id AS user_id,
    fullName AS full_name,
    email,
    signupDate AS signup_date,
    state,
    isGovEmployee AS is_gov_employee,
    updatedAt AS updated_at
FROM ranked
WHERE rn = 1
