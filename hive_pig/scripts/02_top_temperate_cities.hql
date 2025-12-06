-- ==========================================
-- Task 1: Rank Top 10 Most Temperate Cities
-- ==========================================
-- Ranks cities by their average maximum temperature (temperature_2m_max)
-- "Most temperate" = cities with highest average max temperature

-- Create view with joined data
DROP VIEW IF EXISTS weather_with_city;
CREATE VIEW weather_with_city AS
SELECT 
    w.location_id,
    w.temperature_2m_max,
    w.temperature_2m_min,
    w.temperature_2m_mean,
    l.city_name
FROM weather_data w
JOIN location_data l ON w.location_id = l.location_id;

-- Top 10 Most Temperate Cities (highest average max temperature)
SELECT 
    city_name,
    ROUND(AVG(temperature_2m_max), 2) AS avg_max_temperature,
    ROUND(AVG(temperature_2m_min), 2) AS avg_min_temperature,
    ROUND(AVG(temperature_2m_mean), 2) AS avg_mean_temperature,
    COUNT(*) AS total_records
FROM weather_with_city
GROUP BY city_name
ORDER BY avg_max_temperature DESC
LIMIT 10;
