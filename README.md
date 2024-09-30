# Weather Information Data Pipeline & Streamlit

This project extracts weather data from the [National Weather Service API](https://api.weather.gov), processes it, and stores it in a PostgreSQL database. Additionally, it includes a Streamlit application for visualizing weather metrics.

##### Current Metrics:
 * Average temperature for last week (Mon-Sun)
 * Maximum wind speed change between two consecutive observations in the last 7 days


## Table of Contents

- [Technologies Used](#technologies-used)
- [Setup](#setup)
- [Architecture Diagram](#architecture-diagram)
- [Data Model](#data-modeling) 
- [ETL Pipeline](#etl-pipeline)
  - [Overview](#overview)
  - [Airflow DAG](#airflow-dag)
- [Streamlit Application](#streamlit-application)
  - [Features](#features)
  - [Usage](#usage)

## Technologies Used

- Python
- Apache Airflow
- PostgreSQL
- Streamlit
- Docker

## Setup

### Prerequisites

- **Docker**: Ensure that Docker is installed on your machine. You can download it from [Docker's official website](https://www.docker.com/get-started).
- **Docker Compose**: This usually comes bundled with Docker, but ensure it's installed. You can check by running `docker-compose --version`.

### Installation Steps

1. **Clone the repository:**

   ```bash
   git clone https://github.com/kenworthito/weather_information_pipeline.git
   cd weather_information_pipeline
   ```

2. **Build and run the Docker containers:**

   ```bash
   docker-compose up --build
   ```

   This command will start all necessary services, including the Airflow web server, PostgreSQL, and the Streamlit app.

3. **Access the services:**
   - Airflow: `http://localhost:8080` (username: admin | password: admin)
   - Streamlit: `http://localhost:8501`

## Architecture Diagram

![Diagram](/images/architecture-diagram.png)

## Data Model

This project consists of a single table that stores all the necessary data for our application. The table is structured as follows:

### Table: `weather_observations`

| Column Name               | Data Type        | Description                                         |
|---------------------------|------------------|-----------------------------------------------------|
| `id`                      | SERIAL           | Unique identifier for each record (Primary Key).   |
| `station_id`              | VARCHAR(10)      | Identifier for the weather station.                 |
| `station_name`            | VARCHAR(100)     | Name of the weather station.                         |
| `station_timezone`        | VARCHAR(50)      | Time zone of the weather station.                   |
| `latitude`                | DECIMAL(9,6)     | Latitude of the weather station's location.         |
| `longitude`               | DECIMAL(9,6)     | Longitude of the weather station's location.        |
| `observation_timestamp`   | TIMESTAMP        | Timestamp for the observation.                       |
| `temperature`             | DECIMAL(5,2)     | Recorded temperature.                                |
| `temperature_unit_code`   | VARCHAR(20)      | Unit of measurement for temperature.                |
| `wind_speed`              | DECIMAL(5,2)     | Recorded wind speed.                                |
| `wind_speed_unit_code`    | VARCHAR(20)      | Unit of measurement for wind speed.                 |
| `humidity`                | DECIMAL(5,2)     | Recorded humidity level.                             |

### Constraints:
- **Primary Key**: `id`
- **Unique Constraint**: Combination of `station_id` and `observation_timestamp` to ensure that each observation for a specific station is unique at a given time.


## ETL Pipeline

### Overview

The ETL pipeline is managed by Apache Airflow, which orchestrates the extraction, transformation, and loading of weather data. The main steps include:

1. Fetching available weather stations from the API.
2. Retrieving observations for the selected stations.
3. Inserting the data into a PostgreSQL database.

### Airflow DAG

The main workflow is defined in the `weather_etl_pipeline.py` file. Key functions include:

- **fetch_stations**: Fetches weather station data from the API and stores it in XCom.
- **fetch_observations**: Retrieves weather observations for the specified stations and stores them in XCom.
- **insert_data**: Inserts the observations into the PostgreSQL database in batches.

## Streamlit Application

### Features

The Streamlit application provides a user-friendly interface to visualize weather metrics. Key features include:

- Displaying the average temperature for the last week for a selected weather station.
- Showing the maximum change in wind speed between consecutive observations.

### Usage

1. **Run the Streamlit app:**

   Make sure your Docker containers are running, then open the Streamlit app by navigating to:

   ```
   http://localhost:8501
   ```

2. **Select a weather station:**

   From the dropdown menu, select a weather station to view the metrics:

   - **Average Temperature**: Displays the average temperature for the last week.
   - **Maximum Wind Speed Change**: Displays the maximum wind speed change between consecutive observations.

### Notes:
- If the Streamlit app doesn't display data, try executing the Airflow DAG manually to resolve the issue. `http://localhost:8080`