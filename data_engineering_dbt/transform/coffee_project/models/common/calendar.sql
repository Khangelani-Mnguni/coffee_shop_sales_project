WITH dates AS (

    SELECT
        DATE_ADD('2025-01-01', INTERVAL day_num DAY) AS date_day
    FROM UNNEST(GENERATE_ARRAY(0, 365)) AS day_num

)

SELECT
    date_day,
    EXTRACT(YEAR FROM date_day) AS year,
    EXTRACT(MONTH FROM date_day) AS month,
    EXTRACT(DAY FROM date_day) AS day,
    EXTRACT(DAYOFWEEK FROM date_day) AS day_of_week

FROM dates