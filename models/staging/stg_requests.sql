WITH source_data AS (
    SELECT
        REQUEST_NUMBER::STRING AS request_number,
        REQUEST_DESCRIPTION::STRING AS request_description,
        REQUEST_DETAILS::STRING AS request_details,
        TRY_TO_DATE(REQUEST_CREATION_DATE) AS request_creation_date,
        STATUS::STRING AS status,
        TRY_TO_DATE(STATUS_DATE) AS status_date,
        INGESTED_AT AS ingested_at,
        ROW_NUMBER() OVER (
            PARTITION BY REQUEST_NUMBER
            ORDER BY INGESTED_AT DESC
        ) AS row_num
    FROM {{ source('bronze', 'edmonton_requests_raw') }}
    WHERE REQUEST_NUMBER IS NOT NULL
)

SELECT
    request_number,
    request_description,
    request_details,
    request_creation_date,
    status,
    status_date,
    ingested_at
FROM source_data
WHERE row_num = 1