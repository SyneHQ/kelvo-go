-- Calendar-month pickup changes with explicit zero-filled zone/month combinations.
WITH eligible AS (
    SELECT
        CAST(CASE
            WHEN pickup_unix_us < 1548979200000000 THEN 201901
            WHEN pickup_unix_us < 1551398400000000 THEN 201902
            ELSE 201903
        END AS BIGINT) AS month,
        pickup_zone_id, fare_cents
    FROM {trips}
    WHERE source_month BETWEEN 201901 AND 201903
      AND pickup_unix_us >= 1546300800000000
      AND pickup_unix_us < 1554076800000000
),
monthly AS (
    SELECT
        month, pickup_zone_id,
        CAST(COUNT(*) AS BIGINT) AS trips,
        CAST(COUNT(fare_cents) AS BIGINT) AS measured_amount_trips,
        CAST(SUM(COALESCE(fare_cents, 0)) AS BIGINT) AS total_amount_cents
    FROM eligible
    GROUP BY month, pickup_zone_id
),
months AS (
    SELECT 201901 AS month
    UNION ALL SELECT 201902
    UNION ALL SELECT 201903
),
grid AS (
    SELECT
        CAST(m.month AS BIGINT) AS month,
        z.borough, CAST(z.zone_id AS BIGINT) AS zone_id, z.zone AS zone_name
    FROM months m
    CROSS JOIN {zones} z
),
filled AS (
    SELECT
        g.month, g.borough, g.zone_id, g.zone_name,
        CAST(COALESCE(m.trips, 0) AS BIGINT) AS trips,
        CAST(COALESCE(m.measured_amount_trips, 0) AS BIGINT) AS measured_amount_trips,
        CAST(COALESCE(m.total_amount_cents, 0) AS BIGINT) AS total_amount_cents
    FROM grid g
    LEFT JOIN monthly m
      ON g.month = m.month AND g.zone_id = m.pickup_zone_id
),
lagged AS (
    SELECT
        month, borough, zone_id, zone_name,
        trips, measured_amount_trips, total_amount_cents,
        LAG(trips, 1) OVER (
            PARTITION BY zone_id ORDER BY month
        ) AS previous_trips
    FROM filled
),
changes AS (
    SELECT
        month, borough, zone_id, zone_name,
        trips, previous_trips,
        CAST(trips - previous_trips AS BIGINT) AS trip_change,
        measured_amount_trips, total_amount_cents
    FROM lagged
    WHERE month > 201901
),
ranked AS (
    SELECT
        month, borough, zone_id, zone_name,
        trips, previous_trips, trip_change,
        measured_amount_trips, total_amount_cents,
        CAST(ROW_NUMBER() OVER (
            PARTITION BY month, borough
            ORDER BY trip_change DESC, zone_id ASC
        ) AS BIGINT) AS zone_rank
    FROM changes
)
SELECT
    month, borough, zone_id, zone_name,
    trips, previous_trips, trip_change,
    measured_amount_trips, total_amount_cents, zone_rank
FROM ranked
ORDER BY month, borough, zone_rank, zone_id;
