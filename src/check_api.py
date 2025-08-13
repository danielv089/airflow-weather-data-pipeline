# --------------------
# File Name:          check_weather_api.py
# Author:             Dániel Varga
# Created:            2025-08-10
# Last Modified:      2025-08-13
#
# Description:
# This module contains a helper function to check the availability
# of the OpenWeather API and validate the API key stored in Airflow Variables.
# The function performs a test request for Kraków (city ID 3094802).
# --------------------


import requests

from airflow.sdk import Variable


def check_weather_api():
    """
    Check if the OpenWeather API is available and the API key is valid.

    This function makes a test request to the OpenWeather API for a specific city
    (city ID 3094802, Kraków) using the API key stored in Airflow Variables.

    Returns:
        bool: True if the API responds with status code 200, False otherwise.
    
    """
    api_key=Variable.get('open_weather_api', default=None)
    url=f'https://api.openweathermap.org/data/2.5/weather?id=3094802&appid={api_key}&units=metric'
    response=requests.get(url)

    if response.status_code==200:
        return True
    else:
        return False