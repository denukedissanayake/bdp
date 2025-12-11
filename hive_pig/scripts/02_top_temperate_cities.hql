-- Rank Top 10 Hottest Cities

SET hive.cli.print.header=true;

WITH city_data AS (
    SELECT 
        l.city_name,
        AVG(w.temperature_2m_max) as avg_max_temp
    FROM weather_data w
    JOIN location_data l ON w.location_id = l.location_id
    GROUP BY l.city_name
)
SELECT 
    DENSE_RANK() OVER (ORDER BY avg_max_temp DESC) as Rank,
    city_name as City,
    ROUND(avg_max_temp, 2) as Avg_Max_Temp
FROM city_data
ORDER BY Rank ASC, City ASC
LIMIT 10;
