-- ==========================================
-- Task 2: Average Evapotranspiration by Agricultural Season
-- ==========================================
-- Calculates average evapotranspiration (et0_fao_evapotranspiration)
-- for each agricultural season in each district over the years.
--
-- Agricultural Seasons in Sri Lanka:
-- - Maha Season: September to March (monsoon season)
-- - Yala Season: April to August (dry season)

-- Create view with season classification
DROP VIEW IF EXISTS weather_with_season;
CREATE VIEW weather_with_season AS
SELECT 
    w.location_id,
    w.date_str,
    CAST(SPLIT(w.date_str, '/')[0] AS INT) AS month,
    CAST(SPLIT(w.date_str, '/')[2] AS INT) AS year,
    w.et0_fao_evapotranspiration,
    l.city_name AS district,
    CASE 
        WHEN CAST(SPLIT(w.date_str, '/')[0] AS INT) >= 9 OR CAST(SPLIT(w.date_str, '/')[0] AS INT) <= 3 
        THEN 'Maha (Sep-Mar)'
        ELSE 'Yala (Apr-Aug)'
    END AS agricultural_season
FROM weather_data w
JOIN location_data l ON w.location_id = l.location_id;

-- Average Evapotranspiration by Season, District, and Year
SELECT 
    district,
    agricultural_season,
    year,
    ROUND(AVG(et0_fao_evapotranspiration), 4) AS avg_evapotranspiration,
    COUNT(*) AS days_count
FROM weather_with_season
GROUP BY district, agricultural_season, year
ORDER BY district, year, agricultural_season;
