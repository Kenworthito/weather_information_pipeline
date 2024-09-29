def average_temperature_query():
    return """
    SELECT 
    AVG(temperature) AS average_temperature
    FROM weather_observations
    WHERE station_name = %s
          AND observation_timestamp >= date_trunc('week', CURRENT_DATE) - INTERVAL '1 week'
          AND observation_timestamp < date_trunc('week', CURRENT_DATE);
    """

def max_wind_speed_change_query():
    return """
    WITH wind_changes AS (
        SELECT 
            LAG(wind_speed) OVER (ORDER BY observation_timestamp) AS previous_wind_speed,
            wind_speed,
            observation_timestamp
        FROM weather_observations
        WHERE station_name = %s
              AND observation_timestamp >= NOW() - INTERVAL '7 days'
    )
    SELECT MAX(ABS(wind_speed - previous_wind_speed)) AS max_change
    FROM wind_changes
    WHERE previous_wind_speed IS NOT NULL;
    """

def get_station_names_query():
    return """SELECT DISTINCT station_name FROM weather_observations;"""