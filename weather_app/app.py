import streamlit as st
import pandas as pd
from utils.shared import get_db_connection
from utils.sql_queries_app import *
import logging
import os


# Configure logging
log_directory = '/app/logs'
os.makedirs(log_directory, exist_ok=True)
log_file_path = os.path.join(log_directory, 'app_logs.log')

logging.basicConfig(
    filename=log_file_path,
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)


def get_average_temperature(station_name):
    """
    Fetch the average temperature for the last week from the database.

    Args:
        station_name (str): The name of the weather station.

    Returns:
        float: Average temperature in Celsius.
    """
    logger.info(f"Fetching average temperature for station: {station_name}")
    conn = get_db_connection()
    try:
        query = average_temperature_query()
        df = pd.read_sql_query(query, conn, params=(station_name,))
        logger.info(f"Average temperature for {station_name}: {df['average_temperature'].iloc[0]} °C")
        return df['average_temperature'].iloc[0]
    except Exception as e:
        logger.error(f"Error fetching average temperature for {station_name}: {e}")
        st.error(f"Could not fetch average temperature for {station_name}.")
        return None
    finally:
        conn.close()


def get_max_wind_speed_change(station_name):
    """
    Fetch the maximum change in wind speed for the last week from the database.

    Args:
        station_name (str): The name of the weather station.

    Returns:
        float: Maximum wind speed change in km/h.
    """
    logger.info(f"Fetching max wind speed change for station: {station_name}")
    conn = get_db_connection()
    try:
        query = max_wind_speed_change_query()
        df = pd.read_sql_query(query, conn, params=(station_name,))
        logger.info(f"Max wind speed change for {station_name}: {df['max_change'].iloc[0]} km/h")
        return df['max_change'].iloc[0]
    except Exception as e:
        logger.error(f"Error fetching max wind speed change for {station_name}: {e}")
        st.error(f"Could not fetch max wind speed change for {station_name}.")
        return None
    finally:
        conn.close()


def get_station_names():
    """
    Fetch the list of distinct weather station names from the database.

    Returns:
        list: A list of station names.
    """
    logger.info("Fetching distinct weather station names from the database")
    conn = get_db_connection()
    try:
        query = get_station_names_query()
        df = pd.read_sql_query(query, conn)
        station_names = df['station_name'].tolist()
        logger.info(f"Fetched station names: {station_names}")
        return station_names
    except Exception as e:
        logger.error(f"Error fetching station names: {e}")
        st.error("Could not fetch station names.")
        return []
    finally:
        conn.close()


station_names = []
def initialize_station_names():
    """ Inicializa la lista de nombres de estaciones desde la base de datos. """
    global station_names
    station_names = get_station_names()

# initialize_station_names()
# # Título de la aplicación
# st.title("Weather Data Metrics")

# # Seleccionar una station_id
# selected_station = st.selectbox("Select a station ID:", station_names)

# if selected_station:
#     # Display the average temperature
#     avg_temp = get_average_temperature(selected_station)
#     if avg_temp is not None:
#         st.write(f"Average observed temperature for the last week: **{avg_temp:.2f} °C**")

#     # Display the maximum wind speed change
#     max_wind_change = get_max_wind_speed_change(selected_station)
#     if max_wind_change is not None:
#         st.write(f"Maximum wind speed change between consecutive observations: **{max_wind_change:.2f} km/h**")
if __name__ == '__main__':
    initialize_station_names()

    # Título de la aplicación
    st.title("Weather Data Metrics")

    # Seleccionar una station_id
    selected_station = st.selectbox("Select a station ID:", station_names)

    if selected_station:
        # Display the average temperature
        avg_temp = get_average_temperature(selected_station)
        if avg_temp is not None:
            st.write(f"Average observed temperature for the last week: **{avg_temp:.2f} °C**")

        # Display the maximum wind speed change
        max_wind_change = get_max_wind_speed_change(selected_station)
        if max_wind_change is not None:
            st.write(f"Maximum wind speed change between consecutive observations: **{max_wind_change:.2f} km/h**")