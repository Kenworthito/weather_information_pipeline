CREATE TABLE IF NOT EXISTS weather_observations (
    id SERIAL PRIMARY KEY,
    station_id VARCHAR(10),
    station_name VARCHAR(100),
    station_timezone VARCHAR(50),
    latitude DECIMAL(9,6),
    longitude DECIMAL(9,6),
    observation_timestamp TIMESTAMP,
    temperature DECIMAL(5,2),
    temperature_unit_code VARCHAR(20),
    wind_speed DECIMAL(5,2),
    wind_speed_unit_code VARCHAR(20),
    humidity DECIMAL(5,2),
    UNIQUE(station_id, observation_timestamp)
);
