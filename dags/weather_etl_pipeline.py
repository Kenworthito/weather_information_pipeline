from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import requests
import logging
from utils.shared import get_db_connection
from utils.sql_queries_dag import *
from utils.config import *
 

logger = logging.getLogger(__name__)


def fetch_stations(**kwargs):
    """
    Fetches available weather stations from the API and pushes the data to XCom.
    
    Args:
        **kwargs: Airflow context variables.
    
    Raises:
        AirflowException: If the API request fails.
    """
    logger.info("Fetching available weather stations...")
    try:
        response = requests.get(STATIONS_ENDPOINT)
        response.raise_for_status()
        stations = response.json()['features']
        # Store the stations in XCom for downstream tasks
        kwargs['ti'].xcom_push(key='stations', value=stations)
        logger.info(f"Fetched {len(stations)} stations.")
    except requests.Timeout:
        logger.error("Request to fetch stations timed out.")
        raise
    except requests.ConnectionError:
        logger.error("Connection error occurred while fetching stations.")
        raise
    except requests.RequestException as e:
        logger.error(f"Error fetching stations: {e}")
        raise


# Function to fetch observations for specific stations
def fetch_observations(**kwargs):
    """
    Fetches weather observations for specific stations and pushes the data to XCom.

    Args:
        **kwargs: Airflow context variables, including:
            - number_of_stations (int): Number of stations to fetch observations for.
            - start_date_offset (int): Number of days to look back for observations.

    Raises:
        AirflowException: If the API request fails or if there are connection issues.
    """
    stations = kwargs['ti'].xcom_pull(key='stations', task_ids='fetch_stations')
    
    # Limit to the specified number of stations
    number_of_stations = kwargs.get('number_of_stations', 1)
    selected_stations = stations[:number_of_stations]
    # List to store observations for all stations
    observations_all = []
    
    start_date_offset = kwargs.get('start_date_offset', 7)
    end_date = datetime.utcnow()
    start_date = end_date - timedelta(days=start_date_offset)
    logger.info(f"Fetching observations for {len(selected_stations)} stations from {start_date} to {end_date}.")

    for station_info in selected_stations:
        url = f'{STATIONS_ENDPOINT}/{station_info["properties"]["stationIdentifier"]}/observations'
        params = {
            'start': start_date.strftime('%Y-%m-%dT%H:%M:%SZ'),
            'end': end_date.strftime('%Y-%m-%dT%H:%M:%SZ')
        }
        
        try:
            response = requests.get(url, params=params)
            response.raise_for_status()
            observations = response.json()['features']
            observations_all.extend(observations)
            logger.info(f"Fetched {len(observations)} observations for station {station_info['properties']['stationIdentifier']}.")
        except requests.Timeout:
            logger.error(f"Request to fetch observations for {station_info['properties']['stationIdentifier']} timed out.")
            raise
        except requests.ConnectionError:
            logger.error(f"Connection error occurred while fetching observations for {station_info['properties']['stationIdentifier']}.")
            raise
        except requests.RequestException as e:
            logger.error(f"Error fetching observations for station {station_info['properties']['stationIdentifier']}: {e}")
            raise

    # Store all observations in XCom for downstream tasks
    kwargs['ti'].xcom_push(key='observations', value=observations_all)


def insert_data(**kwargs):
    """
    Inserts weather observation data into the PostgreSQL database.

    Args:
        **kwargs: Airflow context variables, including:
            - batch_size (int): Number of records to insert in each batch.

    Raises:
        Exception: If there is an error during database operations, logs the error and rolls back the transaction.
    """
    logger.info("Starting to insert data into the database...")
    observations = kwargs['ti'].xcom_pull(key='observations', task_ids='fetch_observations')
    stations = kwargs['ti'].xcom_pull(key='stations', task_ids='fetch_stations')
    logger.info(f"Preparing to insert {len(observations)} observations.")

    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # Use a dictionary to map station_id to its information
        station_info_dict = {station['properties']['stationIdentifier']: station for station in stations}

        insert_data_batch = []
        batch_size = kwargs.get('batch_size', 500)

        for obs in observations:
            properties = obs['properties']
            
            # Extract the station_id from the URL in properties['station']
            station_url = properties.get('station')
            if not station_url:
                logger.warning(f"'station' not found in observation: {properties}")
                continue

            station_id = station_url.split('/')[-1]

            station_info = station_info_dict.get(station_id)

            if station_info is None:
                logger.warning(f"No station info found for station_id {station_id}, skipping...")
                continue
            
            insert_data_batch.append((
                station_id,
                station_info['properties']['name'],
                station_info['properties']['timeZone'],
                station_info['geometry']['coordinates'][1],
                station_info['geometry']['coordinates'][0],
                properties['timestamp'],
                round(properties.get('temperature', {}).get('value', float('nan')), 2) if properties.get('temperature', {}).get('value') is not None else None,
                properties.get('temperature', {}).get('unitCode', ''),
                round(properties.get('windSpeed', {}).get('value', float('nan')), 2) if properties.get('windSpeed', {}).get('value') is not None else None,
                properties.get('windSpeed', {}).get('unitCode', ''),
                round(properties.get('humidity', {}).get('value', float('nan')), 2) if properties.get('humidity', {}).get('value') is not None else None
            ))

            if len(insert_data_batch) >= batch_size:
                cursor.executemany(INSERT_OBSERVATION_QUERY, insert_data_batch)
                logger.info(f"Inserted {len(insert_data_batch)} records into the database.")
                insert_data_batch = []
                
        if insert_data_batch:
            cursor.executemany(INSERT_OBSERVATION_QUERY, insert_data_batch)
            logger.info(f"Inserted {len(insert_data_batch)} records into the database.")

        conn.commit()
        logger.info("Data inserted successfully.")

    except Exception as e:
        logger.error(f"Database operation error: {e}")
        if conn:
            conn.rollback()
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime.now(),
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

# Define the DAG
with DAG('weather_etl_pipeline', default_args=default_args, schedule_interval='@daily') as dag:
    fetch_stations_task = PythonOperator(
        task_id='fetch_stations',
        python_callable=fetch_stations,
        provide_context=True,
    )

    fetch_observations_task = PythonOperator(
        task_id='fetch_observations',
        python_callable=fetch_observations,
        op_kwargs={'start_date_offset': START_DATE_OFFSET, 'number_of_stations': NUMBER_OF_STATIONS},
        provide_context=True,
    )

    insert_data_task = PythonOperator(
        task_id='insert_data',
        python_callable=insert_data,
        op_kwargs={'batch_size': BATCH_SIZE},
        provide_context=True,
    )

    # Set task dependencies
    fetch_stations_task >> fetch_observations_task >> insert_data_task
