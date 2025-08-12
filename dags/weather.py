from airflow.sdk import dag, task
from datetime import datetime
from airflow.providers.standard.sensors.python import PythonSensor
from src.check_api import check_weather_api
from airflow.sdk import Variable
from src.extract_data import exctract_weather_data
from src.extract_data import combine_weather_data
from src.transform_data import transfrom_weather_data
import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

api_key=Variable.get('open_weather_api', default=None)

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

    checking_api=PythonSensor(
        task_id='checking_api',
        poke_interval=100,
        timeout=3600,
        mode='reschedule',
        python_callable=check_weather_api,
    )

    @task
    def fetch_city_weather_data(city_id: int):
        return exctract_weather_data(city_id, api_key)

    @task 
    def combine_city_weather_data(results):
        return combine_weather_data(results)

    @task
    def transform_city_weather_data(ls):
        df_dim_city, df_dim_weather, df_fact_weather = transfrom_weather_data(ls)

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
    
    truncate_staging = SQLExecuteQueryOperator(
        task_id='truncate_staging',
        conn_id='postgres_weather_db',
        sql="""
            TRUNCATE TABLE staging.city, staging.weather_desc, staging.weather;
           """,
        )


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


