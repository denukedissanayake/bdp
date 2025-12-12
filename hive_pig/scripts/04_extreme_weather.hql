-- Task 4: Total Number of Days with Extreme Weather Events
-- Extreme weather defined as: High Precipitation (> 10mm) AND High Wind Gusts (> 40 km/h)

SET hive.cli.print.header=true;

WITH extreme_events AS (
    SELECT 
        l.city_name,
        COUNT(*) AS extreme_event_days
    FROM weather_data w
    JOIN location_data l ON w.location_id = l.location_id
    WHERE 
        w.precipitation_sum > 10.0 
        AND w.wind_gusts_10m_max > 40.0
    GROUP BY l.city_name
)
SELECT 
    DENSE_RANK() OVER (ORDER BY extreme_event_days DESC) AS rank,
    city_name,
    extreme_event_days
FROM extreme_events
ORDER BY rank ASC, city_name ASC;

-- Query 2: Extreme Weather Events by City and Year
WITH extreme_events_by_year AS (
    SELECT 
        l.city_name,
        year(to_date(w.date_str)) AS event_year,
        COUNT(*) AS extreme_event_days
    FROM weather_data w
    JOIN location_data l ON w.location_id = l.location_id
    WHERE 
        w.precipitation_sum > 10.0 
        AND w.wind_gusts_10m_max > 40.0
    GROUP BY l.city_name, year(to_date(w.date_str))
)
SELECT 
    city_name,
    event_year,
    extreme_event_days
FROM extreme_events_by_year
ORDER BY city_name ASC, event_year ASC;
