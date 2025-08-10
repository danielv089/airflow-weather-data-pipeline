import pandas as pd

def transfrom_weather_data(ls):
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
        'main.humidity': 'humidity_%',
        'main.pressure': 'pressure_hPa',
        'main.temp_max' : 'temp_max_c',
        'main.temp_min': 'temp_min_c',
        'main.sea_level': 'sea_level_hPa',
        'main.feels_like' : 'feels_like_c',
        'main.grnd_level': 'grnd_level_hPa',
        'wind.deg' : 'wind_dir_deg',
        'wind.gust': 'wind_gust_m_s',
        'wind.speed': 'wind_speed_m_s',
        'coord.lat' : 'lat_city',
        'coord.lon' : 'lon_city',
        'clouds.all' : 'cloudiness_%',
        'weather': 'weather_con_id'  
    }

    df_2=df_2.rename(columns=df_rename)
    df_2= df_2.drop(['sys.id', 'sys.type'], axis=1)
    df_2=df_2[['date_time', 'sunrise', 'sunset', 'timezone',
               'city_id','city_name', 'country_code', 'lat_city', 'lon_city',
               'visibility_meters', 'cloudiness_%', 'weather_con_id',
               'temp_c', 'feels_like_c', 'temp_max_c', 'temp_min_c',
               'pressure_hPa', 'sea_level_hPa', 'grnd_level_hPa', 'humidity_%',
               'wind_speed_m_s', 'wind_gust_m_s', 'wind_dir_deg']]
    
    df_2['city_name']=df_2['city_name'].astype(str)
    df_2['country_code']=df_2['country_code'].astype(str)
    df_2['date_time'] = pd.to_datetime(df_2['date_time'], unit='s', utc=True) + pd.to_timedelta(df_2['timezone'], unit='s')
    df_2['sunrise'] = pd.to_datetime(df_2['sunrise'], unit='s', utc=True) + pd.to_timedelta(df_2['timezone'], unit='s')
    df_2['sunset'] = pd.to_datetime(df_2['sunset'], unit='s', utc=True) + pd.to_timedelta(df_2['timezone'], unit='s')
    df_2= df_2.drop(columns=['timezone'])
    df_2 = df_2.reset_index(drop=True)

    weather_codes = [
        {'weather_id': 200, 'weather_main': 'Thunderstorm', 'weather_desc': 'thunderstorm with light rain'},
        {'weather_id': 201, 'weather_main': 'Thunderstorm', 'weather_desc': 'thunderstorm with rain'},
        {'weather_id': 202, 'weather_main': 'Thunderstorm', 'weather_desc': 'thunderstorm with heavy rain'},
        {'weather_id': 210, 'weather_main': 'Thunderstorm', 'weather_desc': 'light thunderstorm'},
        {'weather_id': 211, 'weather_main': 'Thunderstorm', 'weather_desc': 'thunderstorm'},
        {'weather_id': 212, 'weather_main': 'Thunderstorm', 'weather_desc': 'heavy thunderstorm'},
        {'weather_id': 221, 'weather_main': 'Thunderstorm', 'weather_desc': 'ragged thunderstorm'},
        {'weather_id': 230, 'weather_main': 'Thunderstorm', 'weather_desc': 'thunderstorm with light drizzle'},
        {'weather_id': 231, 'weather_main': 'Thunderstorm', 'weather_desc': 'thunderstorm with drizzle'},
        {'weather_id': 232, 'weather_main': 'Thunderstorm', 'weather_desc': 'thunderstorm with heavy drizzle'},
        {'weather_id': 300, 'weather_main': 'Drizzle', 'weather_desc': 'light intensity drizzle'},
        {'weather_id': 301, 'weather_main': 'Drizzle', 'weather_desc': 'drizzle'},
        {'weather_id': 302, 'weather_main': 'Drizzle', 'weather_desc': 'heavy intensity drizzle'},
        {'weather_id': 310, 'weather_main': 'Drizzle', 'weather_desc': 'light intensity drizzle rain'},
        {'weather_id': 311, 'weather_main': 'Drizzle', 'weather_desc': 'drizzle rain'},
        {'weather_id': 312, 'weather_main': 'Drizzle', 'weather_desc': 'heavy intensity drizzle rain'},
        {'weather_id': 313, 'weather_main': 'Drizzle', 'weather_desc': 'shower rain and drizzle'},
        {'weather_id': 314, 'weather_main': 'Drizzle', 'weather_desc': 'heavy shower rain and drizzle'},
        {'weather_id': 321, 'weather_main': 'Drizzle', 'weather_desc': 'shower drizzle'},
        {'weather_id': 500, 'weather_main': 'Rain', 'weather_desc': 'light rain'},
        {'weather_id': 501, 'weather_main': 'Rain', 'weather_desc': 'moderate rain'},
        {'weather_id': 502, 'weather_main': 'Rain', 'weather_desc': 'heavy intensity rain'},
        {'weather_id': 503, 'weather_main': 'Rain', 'weather_desc': 'very heavy rain'},
        {'weather_id': 504, 'weather_main': 'Rain', 'weather_desc': 'extreme rain'},
        {'weather_id': 511, 'weather_main': 'Rain', 'weather_desc': 'freezing rain'},
        {'weather_id': 520, 'weather_main': 'Rain', 'weather_desc': 'light intensity shower rain'},
        {'weather_id': 521, 'weather_main': 'Rain', 'weather_desc': 'shower rain'},
        {'weather_id': 522, 'weather_main': 'Rain', 'weather_desc': 'heavy intensity shower rain'},
        {'weather_id': 531, 'weather_main': 'Rain', 'weather_desc': 'ragged shower rain'},
        {'weather_id': 600, 'weather_main': 'Snow', 'weather_desc': 'light snow'},
        {'weather_id': 601, 'weather_main': 'Snow', 'weather_desc': 'snow'},
        {'weather_id': 602, 'weather_main': 'Snow', 'weather_desc': 'heavy snow'},
        {'weather_id': 611, 'weather_main': 'Snow', 'weather_desc': 'sleet'},
        {'weather_id': 612, 'weather_main': 'Snow', 'weather_desc': 'light shower sleet'},
        {'weather_id': 613, 'weather_main': 'Snow', 'weather_desc': 'shower sleet'},
        {'weather_id': 615, 'weather_main': 'Snow', 'weather_desc': 'light rain and snow'},
        {'weather_id': 616, 'weather_main': 'Snow', 'weather_desc': 'rain and snow'},
        {'weather_id': 620, 'weather_main': 'Snow', 'weather_desc': 'light shower snow'},
        {'weather_id': 621, 'weather_main': 'Snow', 'weather_desc': 'shower snow'},
        {'weather_id': 622, 'weather_main': 'Snow', 'weather_desc': 'heavy shower snow'},
        {'weather_id': 701, 'weather_main': 'Mist', 'weather_desc': 'mist'},
        {'weather_id': 711, 'weather_main': 'Smoke', 'weather_desc': 'smoke'},
        {'weather_id': 721, 'weather_main': 'Haze', 'weather_desc': 'haze'},
        {'weather_id': 731, 'weather_main': 'Dust', 'weather_desc': 'sand/dust whirls'},
        {'weather_id': 741, 'weather_main': 'Fog', 'weather_desc': 'fog'},
        {'weather_id': 751, 'weather_main': 'Sand', 'weather_desc': 'sand'},
        {'weather_id': 761, 'weather_main': 'Dust', 'weather_desc': 'dust'},
        {'weather_id': 762, 'weather_main': 'Ash', 'weather_desc': 'volcanic ash'},
        {'weather_id': 771, 'weather_main': 'Squall', 'weather_desc': 'squalls'},
        {'weather_id': 781, 'weather_main': 'Tornado', 'weather_desc': 'tornado'},
        {'weather_id': 800, 'weather_main': 'Clear', 'weather_desc': 'clear sky'},
        {'weather_id': 801, 'weather_main': 'Clouds', 'weather_desc': 'few clouds: 11-25%'},
        {'weather_id': 802, 'weather_main': 'Clouds', 'weather_desc': 'scattered clouds: 25-50%'},
        {'weather_id': 803, 'weather_main': 'Clouds', 'weather_desc': 'broken clouds: 51-84%'},
        {'weather_id': 804, 'weather_main': 'Clouds', 'weather_desc': 'overcast clouds: 85-100%'}]

    df_dim_city = df_2[['city_id', 'city_name', 'lat_city', 'lon_city', 'country_code']]
    df_dim_weather=pd.DataFrame(weather_codes)
    df_fact_weather = df_2[['date_time', 'sunrise', 'sunset', 'visibility_meters','cloudiness_%', 'weather_con_id', 'temp_c', 'feels_like_c',
                            'temp_max_c', 'temp_min_c', 'pressure_hPa', 'sea_level_hPa', 'grnd_level_hPa', 'humidity_%', 'wind_speed_m_s', 
                            'wind_gust_m_s','wind_dir_deg']]

    return df_dim_city, df_dim_weather, df_fact_weather
