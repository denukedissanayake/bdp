-- Task 1: Rank Top 10 Most Temperate Cities
-- Ranks cities by average maximum temperature (temperature_2m_max)

SET hive.cli.print.header=true;

DROP VIEW IF EXISTS weather_with_city;
CREATE VIEW weather_with_city AS
SELECT 
    w.temperature_2m_max,
    w.temperature_2m_min,
    w.temperature_2m_mean,
    l.city_name
FROM weather_data w
JOIN location_data l ON w.location_id = l.location_id;

SELECT 
    city_name AS City,
    ROUND(AVG(temperature_2m_max), 2) AS Avg_Max_Temp,
    ROUND(AVG(temperature_2m_min), 2) AS Avg_Min_Temp,
    ROUND(AVG(temperature_2m_mean), 2) AS Avg_Mean_Temp,
    COUNT(*) AS Total_Records
FROM weather_with_city
GROUP BY city_name
ORDER BY Avg_Max_Temp DESC
LIMIT 10;
