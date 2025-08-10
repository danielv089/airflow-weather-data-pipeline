from airflow.sdk import dag, task
from datetime import datetime
from airflow.providers.standard.sensors.python import PythonSensor
from src.check_api import check_weather_api
from airflow.sdk import Variable
from src.extract_data import exctract_weather_data
from src.extract_data import combine_weather_data
from src.transform_data import transfrom_weather_data

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
    tags=['weather']
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
        return transfrom_weather_data(ls)





    city_weather_data_extract= fetch_city_weather_data.expand(city_id=list(pl_city_dict.values()))
    city_weather_data_combine= combine_city_weather_data(city_weather_data_extract)
    city_weather_data_transform = transform_city_weather_data(city_weather_data_combine)


    checking_api >> city_weather_data_extract >> city_weather_data_combine >> city_weather_data_transform


weather_data_etl()


