-- Large routes by derived recorded total amount, within an explicit usable-data cohort.
WITH eligible AS (
    SELECT pickup_zone_id, dropoff_zone_id, fare_cents, trip_distance
    FROM {trips}
    WHERE source_month BETWEEN 201901 AND 201903
      AND pickup_unix_us >= 1546300800000000
      AND pickup_unix_us < 1554076800000000
      AND fare_cents IS NOT NULL
      AND fare_cents >= 0
),
classified AS (
    SELECT
        pickup_zone_id, dropoff_zone_id, fare_cents, trip_distance,
        CASE WHEN trip_distance > 0 AND trip_distance <= 100
             THEN 1 ELSE 0 END AS usable_distance
    FROM eligible
),
routes AS (
    SELECT
        pickup_zone_id, dropoff_zone_id,
        CAST(SUM(usable_distance) AS BIGINT) AS trips,
        CAST(SUM(CASE WHEN usable_distance = 1 THEN fare_cents ELSE 0 END) AS BIGINT)
            AS total_amount_cents,
        CAST(SUM(CASE WHEN usable_distance = 1 AND trip_distance < 1
                      THEN 1 ELSE 0 END) AS BIGINT)
            AS lt_1_mile_trips,
        CAST(SUM(CASE WHEN usable_distance = 1 AND trip_distance >= 1 AND trip_distance < 5
                      THEN 1 ELSE 0 END) AS BIGINT) AS ge_1_lt_5_mile_trips,
        CAST(SUM(CASE WHEN usable_distance = 1 AND trip_distance >= 5
                      THEN 1 ELSE 0 END) AS BIGINT)
            AS ge_5_le_100_mile_trips
    FROM classified
    GROUP BY pickup_zone_id, dropoff_zone_id
),
qualified AS (
    SELECT *
    FROM routes
    WHERE trips >= 100
),
named AS (
    SELECT
        p.borough AS pickup_borough,
        CAST(p.zone_id AS BIGINT) AS pickup_zone_id,
        p.zone AS pickup_zone_name,
        d.borough AS dropoff_borough,
        CAST(d.zone_id AS BIGINT) AS dropoff_zone_id,
        d.zone AS dropoff_zone_name,
        r.trips, r.total_amount_cents,
        r.lt_1_mile_trips, r.ge_1_lt_5_mile_trips, r.ge_5_le_100_mile_trips
    FROM qualified r
    INNER JOIN {zones} p ON r.pickup_zone_id = p.zone_id
    INNER JOIN {zones} d ON r.dropoff_zone_id = d.zone_id
),
ranked AS (
    SELECT
        pickup_borough, pickup_zone_id, pickup_zone_name,
        dropoff_borough, dropoff_zone_id, dropoff_zone_name,
        trips, total_amount_cents,
        lt_1_mile_trips, ge_1_lt_5_mile_trips, ge_5_le_100_mile_trips,
        CAST(ROW_NUMBER() OVER (
            PARTITION BY pickup_borough
            ORDER BY total_amount_cents DESC, trips DESC,
                     pickup_zone_id ASC, dropoff_zone_id ASC
        ) AS BIGINT) AS route_rank
    FROM named
)
SELECT
    pickup_borough, pickup_zone_id, pickup_zone_name,
    dropoff_borough, dropoff_zone_id, dropoff_zone_name,
    trips, total_amount_cents,
    lt_1_mile_trips, ge_1_lt_5_mile_trips, ge_5_le_100_mile_trips,
    route_rank
FROM ranked
WHERE route_rank <= 10
ORDER BY pickup_borough, route_rank, pickup_zone_id, dropoff_zone_id;
