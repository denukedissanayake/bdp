-- Task 2: Average Evapotranspiration by Agricultural Season
-- Calculates average evapotranspiration for Sep-Mar and Apr-Aug seasons

SET hive.cli.print.header=true;

DROP VIEW IF EXISTS weather_with_season;
CREATE VIEW weather_with_season AS
SELECT 
    CAST(SPLIT(w.date_str, '/')[0] AS INT) AS month,
    CAST(SPLIT(w.date_str, '/')[2] AS INT) AS year,
    w.et0_fao_evapotranspiration,
    l.city_name AS district,
    CASE 
        WHEN CAST(SPLIT(w.date_str, '/')[0] AS INT) >= 9 OR CAST(SPLIT(w.date_str, '/')[0] AS INT) <= 3 
        THEN 'Season (Sep-Mar)'
        ELSE 'Season (Apr-Aug)'
    END AS agricultural_season
FROM weather_data w
JOIN location_data l ON w.location_id = l.location_id;

SELECT 
    district AS District,
    agricultural_season AS Season,
    year AS Year,
    ROUND(AVG(et0_fao_evapotranspiration), 4) AS Avg_Evapotranspiration
FROM weather_with_season
GROUP BY district, agricultural_season, year
ORDER BY district, year, agricultural_season;
