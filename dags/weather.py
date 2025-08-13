#--------------------
# DAG Name: weather_data_etl
# Author:             Dániel Varga
# Created:            2025-08-10
# Last Modified:      2025-08-013
#
#This Airflow DAG extracts weather data using the OpenWeather API for all voivodeship capital cities in Poland. 
#It checks the API availability, extracts the data, combines it into a unified dataset,
#transforms it into dimensional model tables, and loads it into a PostgreSQL data warehouse.
#Steps:
#1. Check API availability.
#2. Extract weather data for each city.
#3. Combine extracted data into a unified dataset.
#4. Transform the dataset into dimension and fact tables.
#5. Load transformed data into staging tables.
#6. Insert data into core dimension/fact tables, avoiding duplicates for dimensional tables.
#7. Truncate staging tables after loading.

#Schedule: Daily at 06:00 CET (Polish time).
#--------------------

from datetime import datetime

import pandas as pd
from airflow.sdk import dag, task
from airflow.providers.standard.sensors.python import PythonSensor
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.sdk import Variable

from src.check_api import check_weather_api
from src.extract_data import exctract_weather_data
from src.extract_data import combine_weather_data
from src.transform_data import transfrom_weather_data

# Retrieve API key from Airflow Variables (Needs to be set in Airflow UI)
api_key=Variable.get('open_weather_api', default=None)

# Dictionary mapping Polish city names to OpenWeather city IDs to utilize dynamic task mapping for parallel data extraction.
pl_city_dict = {
    'Białystok': 776069,
    'Bydgoszcz': 7530814,
    'Gdańsk': 7531890,
    'Gorzów Wielkopolski': 3098722,
    'Katowice': 3096472,
    'Kielce': 769250,
    'Kraków': 3094802,
    'Lublin': 765876,
    'Łódź': 3093133,
    'Olsztyn': 3090104,
    'Opole': 3090048,
    'Poznań': 3088171,
    'Rzeszów': 759734,
    'Szczecin': 3083829,
    'Toruń': 3083271,
    'Warsaw': 756135,
    'Zielona Góra': 3080165
}

@dag(
    start_date=datetime(2025,8,9),
    schedule='0 6 * * *',
    catchup=False,
    tags=['weather', 'ETL', 'API', 'Python']
    )
def weather_data_etl():
    """
    Airflow DAG to extract, transform, and load daily weather data
    for Polish cities into a PostgreSQL database.
    """

    #Sensor to check if the OpenWeather API is available before proceeding with data extraction.
    checking_api=PythonSensor(
        task_id='checking_api',
        poke_interval=100,
        timeout=3600,
        mode='reschedule',
        python_callable=check_weather_api,
    )

    #Task for extracting weather data for each city using dynamic task mapping, whihc allows parallel execution.
    @task
    def fetch_city_weather_data(city_id: int):
        return exctract_weather_data(city_id, api_key)

    #Task to combine the extracted data into a unified dataset.
    #These tasks utilizes XCOM for passing the information between tasks.
    @task 
    def combine_city_weather_data(results):
        return combine_weather_data(results)

    #Taks for transforming the combined dataset into dimension and fact tables.
    @task
    def transform_city_weather_data(ls):
        df_dim_city, df_dim_weather, df_fact_weather = transfrom_weather_data(ls)

    #Loading the transformed data into staging tables in PostgreSQL.
    load_dim_city_staging= SQLExecuteQueryOperator(
        task_id='load_dim_city_staging',
        conn_id='postgres_weather_db',
        sql="""
            COPY staging.city (
            city_id,
            city_name,
            lat_city,
            lon_city,
            country_code
            )
            FROM '/opt/airflow/data/df_dim_city.csv'
            DELIMITER ','
            CSV HEADER;
            """,
            )
    
    #Loading the transformed data into staging tables in PostgreSQL.
    load_dim_weather_staging= SQLExecuteQueryOperator(
        task_id='load_dim_weather_staging',
        conn_id='postgres_weather_db',
        sql="""
            COPY staging.weather_desc (
            weather_id,
            weather_main,
            weather_desc
            )
            FROM '/opt/airflow/data/df_dim_weather_desc.csv'
            DELIMITER ','
            CSV HEADER;
            """,
            )
    
    #Loading the transformed data into staging tables in PostgreSQL.
    load_fact_weather_staging= SQLExecuteQueryOperator(
        task_id='load_fact_weather_staging',
        conn_id='postgres_weather_db',
        sql="""
            COPY staging.weather (
            date_time,
            sunrise,
            sunset,
            city_id,
            visibility_meters,
            cloudiness_perc,
            weather_con_id,
            temp_c,
            feels_like_c,
            temp_max_c,
            temp_min_c,
            pressure_hpa,
            sea_level_hpa,
            grnd_level_hpa,
            humidity_perc,
            wind_speed_m_s,
            wind_gust_m_s,
            wind_dir_deg
            )
            FROM '/opt/airflow/data/df_fact_weather.csv'
            DELIMITER ','
            CSV HEADER;
            """,
            )
    
    #Loading the data from staging tables into core dimension and fact tables.
    load_dim_city= SQLExecuteQueryOperator(
        task_id='load_dim_city',
        conn_id='postgres_weather_db',
        sql="""
            INSERT INTO core.dim_city (city_id, city_name, lat_city, lon_city, country_code)
            SELECT city_id, city_name, lat_city, lon_city, country_code
            FROM staging.city
            ON CONFLICT (city_id) DO NOTHING;
            """,
            )
    
    #Loading the data from staging tables into core dimension and fact tables.
    load_dim_weather_desc= SQLExecuteQueryOperator(
        task_id='load_dim_weather_desc',
        conn_id='postgres_weather_db',
        sql="""
            INSERT INTO core.dim_weather_desc (weather_id, weather_main, weather_desc)
            SELECT weather_id, weather_main, weather_desc
            FROM staging.weather_desc
            ON CONFLICT (weather_id) DO NOTHING;
            """,
            )
    
    #Loading the data from staging tables into core dimension and fact tables.
    load_fact_weather= SQLExecuteQueryOperator(
        task_id='load_fact_weather',
        conn_id='postgres_weather_db',
        sql="""
            INSERT INTO core.fact_weather (
            date_time, sunrise, sunset, city_id, visibility_meters, cloudiness_perc,
            weather_con_id, temp_c, feels_like_c, temp_max_c, temp_min_c,
            pressure_hpa,sea_level_hpa, grnd_level_hpa, humidity_perc, wind_speed_m_s, wind_gust_m_s, wind_dir_deg
            )
            SELECT 
            date_time, sunrise, sunset, city_id, visibility_meters, cloudiness_perc,
            weather_con_id, temp_c, feels_like_c, temp_max_c, temp_min_c,
            pressure_hpa, grnd_level_hpa, sea_level_hpa, humidity_perc, wind_speed_m_s, wind_gust_m_s, wind_dir_deg
            FROM staging.weather
            """,
            )
    
    #Task to truncate the staging tables after loading the data.
    truncate_staging = SQLExecuteQueryOperator(
        task_id='truncate_staging',
        conn_id='postgres_weather_db',
        sql="""
            TRUNCATE TABLE staging.city, staging.weather_desc, staging.weather;
           """,
        )

    #Task dependency chain to ensure the correct order of execution.
    city_weather_data_extract= fetch_city_weather_data.expand(city_id=list(pl_city_dict.values()))
    city_weather_data_combine= combine_city_weather_data(city_weather_data_extract)
    city_weather_data_transform = transform_city_weather_data(city_weather_data_combine)

    checking_api >> city_weather_data_extract >> city_weather_data_combine >> city_weather_data_transform
    city_weather_data_transform >> load_dim_city_staging >> load_dim_city
    city_weather_data_transform >> load_dim_weather_staging >> load_dim_weather_desc
    city_weather_data_transform >> load_fact_weather_staging
    [load_dim_city, load_dim_weather_desc] >> load_fact_weather_staging >> load_fact_weather
    load_fact_weather >> truncate_staging

weather_data_etl()


