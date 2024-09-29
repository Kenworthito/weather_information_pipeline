import unittest
from unittest.mock import patch, MagicMock
from dags.weather_etl_pipeline import fetch_stations, fetch_observations, insert_data
from utils.sql_queries_dag import *

class TestETL(unittest.TestCase):
    """
    Unit tests for the ETL (Extract, Transform, Load) operations in the weather ETL pipeline.
    This class tests the functions fetch_stations, fetch_observations, and insert_data.
    """

    @patch('dags.weather_etl_pipeline.requests.get')
    def test_fetch_stations(self, mock_requests_get):
        """
        Test the fetch_stations function.

        This test mocks the requests.get method to simulate a successful API call
        that returns a list of weather stations. It checks that the function
        correctly pushes the station data to XCom.

        :param mock_requests_get: Mock object for the requests.get method.
        """
        mock_requests_get.return_value.status_code = 200
        mock_requests_get.return_value.json.return_value = {
            'features': [
                {'properties': {'stationIdentifier': '123'}},
                {'properties': {'stationIdentifier': '456'}}
            ]
        }
        
        mock_kwargs = {'ti': MagicMock()}
        fetch_stations(**mock_kwargs)

        mock_kwargs['ti'].xcom_push.assert_called_once_with(key='stations', value=mock_requests_get.return_value.json.return_value['features'])

    @patch('dags.weather_etl_pipeline.requests.get')
    def test_fetch_observations(self, mock_requests_get):
        """
        Test the fetch_observations function.

        This test mocks the requests.get method to simulate fetching observations
        for the stations retrieved in the fetch_stations test. It checks that
        the correct API call is made and that the function pushes data to XCom.

        :param mock_requests_get: Mock object for the requests.get method.
        """
        mock_kwargs = {
            'ti': MagicMock()
        }
        mock_kwargs['ti'].xcom_pull.return_value = [
            {'properties': {'stationIdentifier': '123'}},
            {'properties': {'stationIdentifier': '456'}}
        ]

        mock_requests_get.return_value.status_code = 200
        mock_requests_get.return_value.json.return_value = {
            'features': [{'properties': {'timestamp': '2024-01-01T00:00:00Z'}}]
        }

        fetch_observations(**mock_kwargs)

        mock_requests_get.assert_called_with('https://api.weather.gov/stations/123/observations', params=unittest.mock.ANY)

        mock_kwargs['ti'].xcom_push.assert_called_once()


    @patch('dags.weather_etl_pipeline.get_db_connection')
    def test_insert_data(self, mock_get_db_connection):
        """
        Test the insert_data function.

        This test mocks the database connection to ensure that the data fetched
        from XCom is inserted correctly into the database. It verifies that
        the cursor's executemany method is called with the correct arguments.

        :param mock_get_db_connection: Mock object for the get_db_connection function.
        """
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_get_db_connection.return_value = mock_conn
        mock_conn.cursor.return_value = mock_cursor

        observations = [
            {
                'properties': {
                    'station': 'http://example.com/station/123',
                    'timestamp': '2024-09-28T23:10:51Z',
                    'temperature': {'value': 20.5, 'unitCode': 'CEL'},
                    'windSpeed': {'value': 5.0, 'unitCode': 'KMH'},
                    'humidity': {'value': 50}
                }
            }
        ]
        
        stations = [
            {
                'properties': {
                    'stationIdentifier': '123',
                    'name': 'Sample Station',
                    'timeZone': 'UTC'
                },
                'geometry': {
                    'coordinates': [-99.1332, 19.4326]
                }
            }
        ]

        mock_ti = MagicMock()
        mock_ti.xcom_pull.side_effect = [observations, stations]

        insert_data(ti=mock_ti)

        mock_cursor.executemany.assert_called_once()
        expected_call_args = [(
            '123',
            'Sample Station',
            'UTC',
            19.4326, 
            -99.1332, 
            '2024-09-28T23:10:51Z',
            20.5,
            'CEL',
            5.0,
            'KMH',
            50
        )]
        mock_cursor.executemany.assert_called_with(INSERT_OBSERVATION_QUERY, expected_call_args)


if __name__ == '__main__':
    unittest.main()


