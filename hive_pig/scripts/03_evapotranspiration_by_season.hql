-- Task 2: Average Evapotranspiration by Agricultural Season
-- Calculates average evapotranspiration for Sep-Mar and Apr-Aug seasons

SET hive.cli.print.header=true;

WITH season_data AS (
    SELECT 
        l.city_name AS district,
        -- Extract Year directly from valid date
        YEAR(FROM_UNIXTIME(UNIX_TIMESTAMP(w.date_str, 'M/d/yyyy'))) AS year,
        -- Define Season Logic based on Month
        CASE 
            WHEN MONTH(FROM_UNIXTIME(UNIX_TIMESTAMP(w.date_str, 'M/d/yyyy'))) BETWEEN 4 AND 8 
            THEN 'Season (Apr-Aug)'
            ELSE 'Season (Sep-Mar)'
        END AS agricultural_season,
        w.et0_fao_evapotranspiration
    FROM weather_data w
    JOIN location_data l ON w.location_id = l.location_id
)
SELECT 
    district,
    agricultural_season,
    year,
    ROUND(AVG(et0_fao_evapotranspiration), 4) AS avg_evapotranspiration
FROM season_data
GROUP BY district, agricultural_season, year
ORDER BY district, year, agricultural_season;
