INSERT_OBSERVATION_QUERY = """
INSERT INTO weather_observations (station_id, station_name, station_timezone, latitude, longitude,
                           observation_timestamp, temperature, temperature_unit_code, wind_speed, wind_speed_unit_code, humidity)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (station_id, observation_timestamp) DO NOTHING;
"""