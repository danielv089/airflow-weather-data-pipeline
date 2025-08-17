# --------------------
# File Name:          transform_data.py
# Author:             Dániel Varga
# Created:            2025-08-10
# Last Modified:      2025-08-17
#
# Description:
# This module contains the helper function to transform raw weather data
# from the OpenWeather API into structured DataFrames suitable
# for loading into PostgreSQL staging and core tables.
# --------------------

from datetime import datetime
import os
import pandas as pd


def transfrom_weather_data(ls):
    """
    Transform raw weather API data into structured DataFrames for ETL.

    Args:
        ls (list of dict): List of JSON objects returned by the weather API for multiple cities.

    Returns:
        tuple: Three DataFrames:
            - df_dim_city: Dimension table for cities.
            - df_dim_weather_desc: Dimension table for weather codes/descriptions.
            - df_fact_weather: Fact table containing detailed weather measurements.

    Steps:
        1. Normalize nested JSON fields into flat columns.
        2. Rename columns to match database schema.
        3. Convert timestamps to proper datetime with timezone adjustment.
        4. Create DataFrames for core and staging tables.
        5. Save CSVs to /opt/airflow/data for loading via Airflow operators.
    """

    df=pd.DataFrame(ls)
    df_sys = df['sys'].apply(pd.Series).add_prefix('sys.')
    df_main = df['main'].apply(pd.Series).add_prefix('main.')
    df_wind = df['wind'].apply(pd.Series).add_prefix('wind.')
    df_coord = df['coord'].apply(pd.Series).add_prefix('coord.')
    df_clouds = df['clouds'].apply(pd.Series).add_prefix('clouds.')
    df_weather=df['weather'].apply(lambda x: x[0]['id'])

    df_2 = pd.concat([df.drop(['sys', 'main', 'wind', 'coord', 'clouds', 'weather', 'cod', 'base'], axis=1),
                      df_sys, df_main, df_wind, df_coord, df_clouds, df_weather], axis=1)
    df_rename={
        'dt': 'date_time',
        'id': 'city_id',
        'name': 'city_name',
        'visibility': 'visibility_meters',
        'sys.sunset': 'sunset',
        'sys.country': 'country_code',
        'sys.sunrise': 'sunrise',
        'main.temp': 'temp_c',
        'main.humidity': 'humidity_perc',
        'main.pressure': 'pressure_hpa',
        'main.temp_max' : 'temp_max_c',
        'main.temp_min': 'temp_min_c',
        'main.sea_level': 'sea_level_hpa',
        'main.feels_like' : 'feels_like_c',
        'main.grnd_level': 'grnd_level_hpa',
        'wind.deg' : 'wind_dir_deg',
        'wind.gust': 'wind_gust_m_s',
        'wind.speed': 'wind_speed_m_s',
        'coord.lat' : 'lat_city',
        'coord.lon' : 'lon_city',
        'clouds.all' : 'cloudiness_perc',
        'weather': 'weather_con_id'  
    }

    df_2=df_2.rename(columns=df_rename)
    df_2= df_2.drop(['sys.id', 'sys.type'], axis=1)
    df_2=df_2[['date_time', 'sunrise', 'sunset', 'city_id', 'timezone',
               'city_name', 'country_code', 'lat_city', 'lon_city',
               'visibility_meters', 'cloudiness_perc', 'weather_con_id',
               'temp_c', 'feels_like_c', 'temp_max_c', 'temp_min_c',
               'pressure_hpa', 'sea_level_hpa', 'grnd_level_hpa', 'humidity_perc',
               'wind_speed_m_s', 'wind_gust_m_s', 'wind_dir_deg']]
    
    df_2['city_name']=df_2['city_name'].astype(str)
    df_2['country_code']=df_2['country_code'].astype(str)
    df_2['date_time'] = pd.to_datetime(df_2['date_time'], unit='s', utc=True) + pd.to_timedelta(df_2['timezone'], unit='s')
    df_2['sunrise'] = pd.to_datetime(df_2['sunrise'], unit='s', utc=True) + pd.to_timedelta(df_2['timezone'], unit='s')
    df_2['sunset'] = pd.to_datetime(df_2['sunset'], unit='s', utc=True) + pd.to_timedelta(df_2['timezone'], unit='s')
    df_2= df_2.drop(columns=['timezone'])
    df_2 = df_2.reset_index(drop=True)

    weather_cond_url='https://openweathermap.org/weather-conditions'
    weather_tables = pd.read_html(weather_cond_url)
    df_weather_desc = pd.concat(weather_tables[1:8], ignore_index=True)
    df_weather_desc = df_weather_desc[[0, 1, 2]]
    df_weather_desc = df_weather_desc.rename(columns={0: 'weather_id', 1 : 'weather_main', 2: 'weather_desc'})
    df_weather_desc['weather_main']= df_weather_desc['weather_main'].astype(str)
    df_weather_desc['weather_desc']= df_weather_desc['weather_desc'].astype(str)

    df_dim_city = df_2[['city_id', 'city_name', 'lat_city', 'lon_city', 'country_code']]
    df_dim_weather_desc = df_weather_desc
    df_fact_weather = df_2[['date_time', 'sunrise', 'sunset', 'city_id', 'visibility_meters','cloudiness_perc', 'weather_con_id', 'temp_c', 'feels_like_c',
                            'temp_max_c', 'temp_min_c', 'pressure_hpa', 'sea_level_hpa', 'grnd_level_hpa', 'humidity_perc', 'wind_speed_m_s', 
                            'wind_gust_m_s','wind_dir_deg']]
    
    os.makedirs('/opt/airflow/data', exist_ok=True)
    df_dim_city.to_csv(f"/opt/airflow/data/df_dim_city.csv", index=False)
    df_dim_weather_desc.to_csv(f"/opt/airflow/data/df_dim_weather_desc.csv", index=False)
    df_fact_weather.to_csv(f"/opt/airflow/data/df_fact_weather.csv", index=False)

    return df_dim_city, df_dim_weather_desc, df_fact_weather
