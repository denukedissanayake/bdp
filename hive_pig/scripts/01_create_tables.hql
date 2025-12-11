-- Drop existing tables if they exist
DROP TABLE IF EXISTS weather_data;
DROP TABLE IF EXISTS location_data;

-- Create Location Data Table
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
LOCATION '/user/data/input/location_data'
TBLPROPERTIES ('skip.header.line.count'='1');

-- Create Weather Data Table
-- Columns: location_id, date, weather_code, temperature_2m_max, temperature_2m_min, temperature_2m_mean, apparent_temperature_max, apparent_temperature_min, apparent_temperature_mean, daylight_duration, sunshine_duration, precipitation_sum, rain_sum, precipitation_hours, wind_speed_10m_max, wind_gusts_10m_max, wind_direction_10m_dominant, shortwave_radiation_sum, et0_fao_evapotranspiration, sunrise, sunset

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
LOCATION '/user/data/input/weather_data'
TBLPROPERTIES ('skip.header.line.count'='1');

-- Verify tables created successfully
SHOW TABLES;
DESCRIBE weather_data;
DESCRIBE location_data;
