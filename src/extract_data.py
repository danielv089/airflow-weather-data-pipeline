# --------------------
# File Name:          extract_data.py
# Author:             Dániel Varga
# Created:            2025-08-10
# Last Modified:      2025-08-13
#
# Description:
# This module contains functions to extract and combine weather data
# from the OpenWeather API.
# --------------------

import requests

def exctract_weather_data(city_id: int, api_key):
    """
    Fetch weather data from the OpenWeather API for a given city.

    Args:
        city_id (int): The OpenWeather city ID.
        api_key (str): The API key for authentication.

    Returns:
        dict: JSON data from the API containing weather information.

    Raises:
        requests.RequestException: If the API request fails.
    """

    url=f'https://api.openweathermap.org/data/2.5/weather?id={city_id}&appid={api_key}&units=metric'

    try:
        response=requests.get(url)
        data=response.json()
        return data
    
    except requests.RequestException as e:
        print(f"Error fetching data for city ID {city_id}: {e}")
        raise


def combine_weather_data(results):
    """
    Combine multiple city weather data results into a single list.

    Args:
        results (list of dict): List of JSON weather data for multiple cities.

    Returns:
        list of dict: Flattened list containing all city weather data.
    """
    
    data=[]
    for result in results:
        data.append(result)
    return data