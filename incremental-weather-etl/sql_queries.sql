-- Total number of weather records
SELECT COUNT(*) AS total_rows 
FROM main.weather;

-- Number of records per city, ordered by count descending
SELECT city, COUNT(*) AS record_count
FROM main.weather
GROUP BY city
ORDER BY record_count DESC;

-- Latest observation timestamp for each city
SELECT city, MAX(dt) AS latest_timestamp
FROM main.weather
GROUP BY city;

-- Average temperature and humidity per city
SELECT city,
       ROUND(AVG(temp_c), 2) AS avg_temp_c,
       ROUND(AVG(rh), 2) AS avg_humidity_pct
FROM main.weather
GROUP BY city
ORDER BY avg_temp_c DESC;

-- Count of hours with measurable precipitation per city
SELECT city, COUNT(*) AS rainy_hours
FROM main.weather
WHERE precip_mm > 0
GROUP BY city;

-- Average probability of precipitation per city
SELECT city, ROUND(AVG(pop_pct), 1) AS avg_pop_pct
FROM main.weather
GROUP BY city;

-- Identify any duplicate records based on _id
SELECT _id, COUNT(*) AS duplicates
FROM main.weather
GROUP BY _id
HAVING COUNT(*) > 1;


-- Recent monthly averages for temperature, humidity, and precipitation
SELECT city,
       ROUND(AVG(temp_c), 1) AS avg_temp_c,
       ROUND(AVG(rh), 1) AS avg_humidity_pct,
       ROUND(AVG(precip_mm), 2) AS avg_precip_mm
FROM main.weather
WHERE dt::DATE >= DATE '2025-08-01'
GROUP BY city
ORDER BY avg_temp_c DESC;

---City-level temperature variability
SELECT
  city,
  ROUND(MAX(temp_c) - MIN(temp_c), 2) AS temp_range_c
FROM main.weather
GROUP BY city
ORDER BY temp_range_c DESC;

---Daily average temperature trend
SELECT
  city,
  dt::DATE AS day,
  ROUND(AVG(temp_c), 2) AS daily_avg_temp
FROM main.weather
GROUP BY city, day
ORDER BY city, day;

----Top 5 hottest records per city
SELECT *
FROM (
  SELECT
    city,
    dt,
    temp_c,
    ROW_NUMBER() OVER (PARTITION BY city ORDER BY temp_c DESC) AS rn
  FROM main.weather
)
WHERE rn <= 5;

