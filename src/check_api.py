import requests
from airflow.sdk import Variable


def check_weather_api():
    api_key=Variable.get('open_weather_api', default=None)
    url=f'https://api.openweathermap.org/data/2.5/weather?id=3094802&appid={api_key}&units=metric'
    response=requests.get(url)

    if response.status_code==200:
        return True
    else:
        return False