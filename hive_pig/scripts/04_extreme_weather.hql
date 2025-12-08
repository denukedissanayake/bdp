-- Task 4.4: The total number of days with extreme weather events
-- Defined as: High Precipitation (> 10mm) AND High Wind Gusts (> 40km/h)
-- Note: Adjust thresholds as per specific "extreme" definitions if needed.

SET hive.cli.print.header=true;

USE default;

DROP VIEW IF EXISTS extreme_weather_events;

CREATE VIEW extreme_weather_events AS
SELECT 
    l.district,
    count(*) as extreme_event_days
FROM 
    weather w
JOIN 
    locations l ON w.location_id = l.id
WHERE 
    w.precipitation_sum > 10.0 
    AND w.wind_gusts_10m_max > 40.0
GROUP BY 
    l.district
ORDER BY 
    extreme_event_days DESC;

-- Output the results
SELECT * FROM extreme_weather_events;
