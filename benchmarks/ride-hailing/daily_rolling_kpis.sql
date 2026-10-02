-- Daily recorded-trip KPIs, seven calendar-day windows include zero-trip days.
WITH eligible AS (
    SELECT
        CAST(pickup_unix_us // 86400000000 AS BIGINT) AS day_bucket,
        fare_cents
    FROM {trips}
    WHERE source_month BETWEEN 201901 AND 201903
      AND pickup_unix_us >= 1546300800000000
      AND pickup_unix_us < 1554076800000000
),
daily AS (
    SELECT
        day_bucket,
        CAST(COUNT(*) AS BIGINT) AS trips,
        CAST(COUNT(fare_cents) AS BIGINT) AS measured_amount_trips,
        CAST(SUM(COALESCE(fare_cents, 0)) AS BIGINT) AS total_amount_cents
    FROM eligible
    GROUP BY day_bucket
),
day_spine AS (
    SELECT CAST(range AS BIGINT) AS day_bucket
    FROM range(17897, 17987)
),
filled AS (
    SELECT
        s.day_bucket,
        CAST(COALESCE(d.trips, 0) AS BIGINT) AS trips,
        CAST(COALESCE(d.measured_amount_trips, 0) AS BIGINT) AS measured_amount_trips,
        CAST(COALESCE(d.total_amount_cents, 0) AS BIGINT) AS total_amount_cents
    FROM day_spine s
    LEFT JOIN daily d ON s.day_bucket = d.day_bucket
),
rolling AS (
    SELECT
        day_bucket, trips, measured_amount_trips, total_amount_cents,
        CAST(COUNT(*) OVER trailing_week AS BIGINT) AS window_days,
        CAST(SUM(trips) OVER trailing_week AS BIGINT) AS trailing_7d_trips,
        CAST(SUM(measured_amount_trips) OVER trailing_week AS BIGINT)
            AS trailing_7d_measured_amount_trips,
        CAST(SUM(total_amount_cents) OVER trailing_week AS BIGINT)
            AS trailing_7d_total_amount_cents
    FROM filled
    WINDOW trailing_week AS (
        ORDER BY day_bucket ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    )
)
SELECT
    day_bucket, trips, measured_amount_trips, total_amount_cents,
    window_days, trailing_7d_trips, trailing_7d_measured_amount_trips,
    trailing_7d_total_amount_cents
FROM rolling
ORDER BY day_bucket;
