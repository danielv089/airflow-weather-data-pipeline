import requests

def exctract_weather_data(city_id: int, api_key):
    url=f'https://api.openweathermap.org/data/2.5/weather?id={city_id}&appid={api_key}&units=metric'

    try:
        response=requests.get(url)
        data=response.json()
        return data
    
    except requests.RequestException as e:
        print(f"Error fetching data for city ID {city_id}: {e}")
        raise


def combine_weather_data(results):
    data=[]
    for result in results:
        data.append(result)
    return data