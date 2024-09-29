import unittest
from unittest.mock import patch
import pandas as pd
from weather_app.app import get_average_temperature, get_max_wind_speed_change, get_station_names

class TestWeatherApp(unittest.TestCase):
    """
    Unit tests for the weather application functions that interact with the database.
    This class tests the functions get_average_temperature, get_max_wind_speed_change, and get_station_names.
    """

    @patch('weather_app.app.get_db_connection')
    def test_get_average_temperature(self, mock_get_db_connection):
        """
        Test the get_average_temperature function.

        This test mocks the database connection to simulate a database query that returns
        an average temperature for a specific weather station. It checks that the function
        correctly retrieves the average temperature.

        :param mock_get_db_connection: Mock object for the get_db_connection function.
        """
        mock_conn = mock_get_db_connection.return_value
        mock_conn.cursor.return_value.__enter__.return_value.fetchall.return_value = [(20.5,)]
        pd.read_sql_query = lambda query, con, params: pd.DataFrame({'average_temperature': [20.5]})

        result = get_average_temperature("Sample Station")
        self.assertEqual(result, 20.5)


    @patch('weather_app.app.get_db_connection') 
    def test_get_max_wind_speed_change(self, mock_get_db_connection):
        """
        Test the get_max_wind_speed_change function.

        This test mocks the database connection to simulate a database query that returns
        the maximum change in wind speed for a specific weather station. It checks that
        the function retrieves the correct maximum wind speed change.

        :param mock_get_db_connection: Mock object for the get_db_connection function.
        """
        mock_conn = mock_get_db_connection.return_value
        mock_conn.cursor.return_value.__enter__.return_value.fetchall.return_value = [(5.0,)]
        pd.read_sql_query = lambda query, con, params: pd.DataFrame({'max_change': [5.0]})

        result = get_max_wind_speed_change("Sample Station")
        self.assertEqual(result, 5.0)


if __name__ == '__main__':
    unittest.main()
