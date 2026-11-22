SELECT
    status,
    COUNT(*) AS total_requests,
    COUNT(CASE WHEN status_date IS NOT NULL THEN 1 END) AS with_status_date,
    MIN(status_date) AS earliest_status_date,
    MAX(status_date) AS latest_status_date
FROM {{ ref('stg_requests') }}
GROUP BY status
ORDER BY total_requests DESC