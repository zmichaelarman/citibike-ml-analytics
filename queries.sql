-- name: clean_trips
-- Clean the raw CSVs once and save them as Parquet; every later query reads that file.
COPY (
    WITH parsed AS (
        SELECT
            ride_id,
            rideable_type,
            TRY_CAST(started_at AS TIMESTAMP) AS started_at,
            TRY_CAST(ended_at AS TIMESTAMP) AS ended_at,
            start_station_name,
            NULLIF(TRIM(start_station_id), '') AS start_station_id,
            end_station_name,
            NULLIF(TRIM(end_station_id), '') AS end_station_id,
            TRY_CAST(start_lat AS DOUBLE) AS start_lat,
            TRY_CAST(start_lng AS DOUBLE) AS start_lng,
            TRY_CAST(end_lat AS DOUBLE) AS end_lat,
            TRY_CAST(end_lng AS DOUBLE) AS end_lng,
            member_casual
        FROM read_csv('data/raw/**/*.csv', all_varchar = true, union_by_name = true)
    )
    -- some IDs lost their trailing zero in the export ("5980.1" is station "5980.10"), so pad them back
    SELECT * REPLACE (
        CASE WHEN regexp_full_match(start_station_id, '\d+\.\d') THEN start_station_id || '0' ELSE start_station_id END AS start_station_id,
        CASE WHEN regexp_full_match(end_station_id, '\d+\.\d') THEN end_station_id || '0' ELSE end_station_id END AS end_station_id
    )
    FROM parsed
    WHERE ended_at - started_at BETWEEN INTERVAL 60 SECOND AND INTERVAL 6 HOUR
      -- keep only the 3 full calendar months (the June file also holds a few trips from late May)
      AND started_at >= (SELECT DATE_TRUNC('month', MAX(started_at)) - INTERVAL 2 MONTH FROM parsed)
    QUALIFY ROW_NUMBER() OVER (PARTITION BY ride_id ORDER BY started_at) = 1
) TO 'data/trips.parquet' (FORMAT parquet);

-- name: hourly_trips
-- Trips started per date and hour of day, for the whole system.
SELECT CAST(started_at AS DATE) AS date, HOUR(started_at) AS hour, COUNT(*) AS trips
FROM 'data/trips.parquet'
GROUP BY ALL
ORDER BY date, hour;

-- name: daily_trips
-- Total trips started per date.
SELECT CAST(started_at AS DATE) AS date, COUNT(*) AS trips
FROM 'data/trips.parquet'
GROUP BY ALL
ORDER BY date;

-- name: morning_drainers
-- Top 10 stations by bikes lost (departures - arrivals) per non-holiday weekday, 7-10 am.
WITH weekdays AS (
    SELECT DISTINCT CAST(started_at AS DATE) AS date
    FROM 'data/trips.parquet'
    WHERE ISODOW(started_at) <= 5 AND CAST(started_at AS DATE) NOT IN (SELECT date FROM holidays)
),
events AS (  -- one row per bike leaving (-1) or arriving at (+1) a station
    SELECT start_station_id AS station_id, start_station_name AS name, started_at AS ts, -1 AS change
    FROM 'data/trips.parquet' WHERE start_station_id IS NOT NULL
    UNION ALL
    SELECT end_station_id, end_station_name, ended_at, 1
    FROM 'data/trips.parquet' WHERE end_station_id IS NOT NULL
)
SELECT station_id, MODE(name) AS name,
       -SUM(change) / (SELECT COUNT(*) FROM weekdays) AS bikes_lost_per_weekday
FROM events
WHERE CAST(ts AS DATE) IN (SELECT date FROM weekdays) AND HOUR(ts) BETWEEN 7 AND 9
GROUP BY station_id
ORDER BY bikes_lost_per_weekday DESC
LIMIT 10;

-- name: stations
-- One row per station: most common name, median location, and average trips (departures + arrivals) per day.
WITH events AS (
    SELECT start_station_id AS station_id, start_station_name AS name, start_lat AS lat, start_lng AS lng
    FROM 'data/trips.parquet' WHERE start_station_id IS NOT NULL
    UNION ALL
    SELECT end_station_id, end_station_name, end_lat, end_lng
    FROM 'data/trips.parquet' WHERE end_station_id IS NOT NULL
)
SELECT station_id, MODE(name) AS name, MEDIAN(lat) AS lat, MEDIAN(lng) AS lng,
       COUNT(*) / (SELECT COUNT(DISTINCT CAST(started_at AS DATE)) FROM 'data/trips.parquet') AS trips_per_day
FROM events
WHERE station_id NOT LIKE 'JC%' AND station_id NOT LIKE 'HB%'  -- NJ stations only appear as trip ends in the NYC files
GROUP BY station_id
ORDER BY trips_per_day DESC;

-- name: station_hour_profile
-- Each station's average net flow (arrivals - departures) per hour of day on non-holiday weekdays, long format.
WITH weekdays AS (
    SELECT DISTINCT CAST(started_at AS DATE) AS date
    FROM 'data/trips.parquet'
    WHERE ISODOW(started_at) <= 5 AND CAST(started_at AS DATE) NOT IN (SELECT date FROM holidays)
),
events AS (  -- one row per bike leaving (-1) or arriving at (+1) a station
    SELECT start_station_id AS station_id, started_at AS ts, -1 AS change
    FROM 'data/trips.parquet' WHERE start_station_id IS NOT NULL
    UNION ALL
    SELECT end_station_id, ended_at, 1
    FROM 'data/trips.parquet' WHERE end_station_id IS NOT NULL
)
SELECT station_id, HOUR(ts) AS hour, SUM(change) / (SELECT COUNT(*) FROM weekdays) AS net_flow
FROM events
WHERE CAST(ts AS DATE) IN (SELECT date FROM weekdays)
GROUP BY ALL
ORDER BY station_id, hour;

-- name: typical_weekday
-- Each station's running net flow since 05:00, in 15-minute steps, on an average non-holiday weekday (for the GIF).
WITH weekdays AS (
    SELECT DISTINCT CAST(started_at AS DATE) AS date
    FROM 'data/trips.parquet'
    WHERE ISODOW(started_at) <= 5 AND CAST(started_at AS DATE) NOT IN (SELECT date FROM holidays)
),
events AS (  -- one row per bike leaving (-1) or arriving at (+1) a station
    SELECT start_station_id AS station_id, started_at AS ts, -1 AS change
    FROM 'data/trips.parquet' WHERE start_station_id IS NOT NULL
    UNION ALL
    SELECT end_station_id, ended_at, 1
    FROM 'data/trips.parquet' WHERE end_station_id IS NOT NULL
),
slot_flow AS (  -- slot = which 15-minute block of the day (0-95)
    SELECT station_id, HOUR(ts) * 4 + MINUTE(ts) // 15 AS slot,
           SUM(change) / (SELECT COUNT(*) FROM weekdays) AS net_flow
    FROM events
    WHERE CAST(ts AS DATE) IN (SELECT date FROM weekdays)
    GROUP BY ALL
),
grid AS (  -- every station x every slot from 05:00 (slot 20) to 23:45 (slot 95)
    SELECT station_id, slot
    FROM (SELECT DISTINCT station_id FROM slot_flow), generate_series(20, 95) AS s(slot)
)
SELECT g.station_id,
       printf('%02d:%02d', g.slot // 4, (g.slot % 4) * 15) AS time_label,
       -- bikes gained since 05:00, up to (not including) this slot, so 05:00 starts at zero
       COALESCE(SUM(f.net_flow) OVER (PARTITION BY g.station_id ORDER BY g.slot
                                      ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING), 0) AS cum_net
FROM grid g LEFT JOIN slot_flow f USING (station_id, slot)
ORDER BY g.station_id, g.slot;

-- name: daily_station_rides
-- Departures per station per date, zero-filled from each station's first to last departure date.
WITH rides AS (  -- trips with no end station still count as departures
    SELECT start_station_id AS station_id, CAST(started_at AS DATE) AS date, COUNT(*) AS rides
    FROM 'data/trips.parquet'
    WHERE start_station_id IS NOT NULL
    GROUP BY ALL
),
spans AS (
    SELECT station_id, MIN(date) AS first_date, MAX(date) AS last_date
    FROM rides
    GROUP BY station_id
),
grid AS (  -- one row for every date in each station's span
    SELECT station_id, CAST(UNNEST(generate_series(first_date, last_date, INTERVAL 1 DAY)) AS DATE) AS date
    FROM spans
)
SELECT g.station_id, g.date, COALESCE(r.rides, 0) AS rides
FROM grid g LEFT JOIN rides r USING (station_id, date)
ORDER BY g.station_id, g.date;
