-- Task 1: Rank Top 10 Most Temperate Cities
-- Definition of Temperate:
-- 1. Moderate average temperature (10°C - 25°C)
-- 2. Most stable climate (Lowest Standard Deviation of max temperature)

SET hive.cli.print.header=true;

-- Creating a temporary "virtual table" named city_data
WITH city_data AS (
    SELECT 
        l.city_name,
        AVG(w.temperature_2m_max) as avg_temp,
        STDDEV(w.temperature_2m_max) as temp_variation
    FROM weather_data w
    JOIN location_data l ON w.location_id = l.location_id
    GROUP BY l.city_name
    HAVING avg_temp BETWEEN 10 AND 25
)
-- Select top 10 most stable cities with explicit rank
SELECT 
    DENSE_RANK() OVER (ORDER BY temp_variation ASC) as Rank,
    city_name as City,
    ROUND(avg_temp, 2) as Avg_Temp,
    ROUND(temp_variation, 2) as Variation
FROM city_data
LIMIT 10;
