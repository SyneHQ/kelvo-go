-- Top three pickup zones within each recorded hour and pickup borough.
WITH eligible AS (
    SELECT
        CAST(pickup_unix_us // 3600000000 AS BIGINT) AS hour_bucket,
        pickup_zone_id
    FROM {trips}
    WHERE source_month BETWEEN 201901 AND 201903
      AND pickup_unix_us >= 1546300800000000
      AND pickup_unix_us < 1554076800000000
),
hourly AS (
    SELECT
        e.hour_bucket, z.borough,
        CAST(z.zone_id AS BIGINT) AS zone_id,
        z.zone AS zone_name,
        CAST(COUNT(*) AS BIGINT) AS trips
    FROM eligible e
    INNER JOIN {zones} z ON e.pickup_zone_id = z.zone_id
    GROUP BY e.hour_bucket, z.borough, z.zone_id, z.zone
),
ranked AS (
    SELECT
        hour_bucket, borough, zone_id, zone_name, trips,
        CAST(SUM(trips) OVER (
            PARTITION BY hour_bucket, borough
        ) AS BIGINT) AS borough_hour_trips,
        CAST(ROW_NUMBER() OVER (
            PARTITION BY hour_bucket, borough
            ORDER BY trips DESC, zone_id ASC
        ) AS BIGINT) AS zone_rank
    FROM hourly
)
SELECT
    hour_bucket, borough, zone_id, zone_name,
    trips, borough_hour_trips, zone_rank
FROM ranked
WHERE zone_rank <= 3
ORDER BY hour_bucket, borough, zone_rank, zone_id;
