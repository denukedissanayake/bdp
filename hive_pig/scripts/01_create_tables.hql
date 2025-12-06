-- ==========================================
-- Hive Weather Analytics - Table Definitions
-- ==========================================
-- This script creates external tables for weather and location data

-- Drop existing tables if they exist
DROP TABLE IF EXISTS weather_data;
DROP TABLE IF EXISTS location_data;

-- ==========================================
-- Create Location Data Table
-- ==========================================
-- Columns: location_id, latitude, longitude, elevation, utc_offset_seconds, timezone, timezone_abbreviation, city_name
CREATE EXTERNAL TABLE IF NOT EXISTS location_data (
    location_id INT,
    latitude DOUBLE,
    longitude DOUBLE,
    elevation DOUBLE,
    utc_offset_seconds INT,
    timezone STRING,
    timezone_abbreviation STRING,
    city_name STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/user/hive/warehouse/location_data'
TBLPROPERTIES ('skip.header.line.count'='1');

-- ==========================================
-- Create Weather Data Table
-- ==========================================
-- Columns from weatherData.csv:
-- 0: location_id
-- 1: date (M/d/yyyy format)
-- 2: weather_code
-- 3: temperature_2m_max
-- 4: temperature_2m_min
-- 5: temperature_2m_mean
-- 6: apparent_temperature_max
-- 7: apparent_temperature_min
-- 8: apparent_temperature_mean
-- 9: daylight_duration
-- 10: sunshine_duration
-- 11: precipitation_sum
-- 12: rain_sum
-- 13: precipitation_hours
-- 14: wind_speed_10m_max
-- 15: wind_gusts_10m_max
-- 16: wind_direction_10m_dominant
-- 17: shortwave_radiation_sum
-- 18: et0_fao_evapotranspiration
-- 19: sunrise
-- 20: sunset

CREATE EXTERNAL TABLE IF NOT EXISTS weather_data (
    location_id INT,
    date_str STRING,
    weather_code INT,
    temperature_2m_max DOUBLE,
    temperature_2m_min DOUBLE,
    temperature_2m_mean DOUBLE,
    apparent_temperature_max DOUBLE,
    apparent_temperature_min DOUBLE,
    apparent_temperature_mean DOUBLE,
    daylight_duration DOUBLE,
    sunshine_duration DOUBLE,
    precipitation_sum DOUBLE,
    rain_sum DOUBLE,
    precipitation_hours DOUBLE,
    wind_speed_10m_max DOUBLE,
    wind_gusts_10m_max DOUBLE,
    wind_direction_10m_dominant DOUBLE,
    shortwave_radiation_sum DOUBLE,
    et0_fao_evapotranspiration DOUBLE,
    sunrise STRING,
    sunset STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/user/hive/warehouse/weather_data'
TBLPROPERTIES ('skip.header.line.count'='1');

-- Verify tables created successfully
SHOW TABLES;
DESCRIBE weather_data;
DESCRIBE location_data;
