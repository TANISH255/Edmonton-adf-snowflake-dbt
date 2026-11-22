SELECT
    status_category,
    status,
    COUNT(*) AS request_count,
    COUNT(DISTINCT request_number) AS unique_requests,
    AVG(days_to_resolution) AS avg_days_to_resolution,
    MIN(request_creation_date) AS first_request_date,
    MAX(request_creation_date) AS last_request_date
FROM {{ ref('int_request_metrics') }}
GROUP BY
    status_category,
    status
ORDER BY
    request_count DESC