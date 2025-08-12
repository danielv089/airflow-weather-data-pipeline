from airflow.providers.postgres.hooks.postgres import PostgresHook

def create_weather_pg_db():

    hook = PostgresHook(postgres_conn_id='postgres_conn')
    conn = hook.get_conn()
    conn.autocommit = True
    
    with conn.cursor() as cursor:
        cursor.execute("SELECT 1 FROM pg_database WHERE datname = 'poland_weather_db'")
        exists = cursor.fetchone()

        if not exists:
            cursor.execute("CREATE DATABASE poland_weather_db")
        else:
            print('Database already exusts')

    conn.close() 

def create_core_weather_tables():

    hook= PostgresHook(postgres_conn_id='postgres_weather_db')
    conn= hook.get_conn()
    conn.autocommit = True

    with conn.cursor() as cursor:
        cursor.execute("""
                       CREATE TABLE IF NOT EXISTS core.dim_city (
                       city_id INTEGER PRIMARY KEY,
                       city_name VARCHAR(30) NOT NULL,
                       lat_city DOUBLE PRECISION NOT NULL,
                       lon_city DOUBLE PRECISION NOT NULL,
                       country_code VARCHAR(5) NOT NULL
                       );
                       """)
        
        cursor.execute("""
                        CREATE TABLE IF NOT EXISTS core.dim_weather_desc(
                        weather_id INTEGER PRIMARY KEY,
                        weather_main VARCHAR(30) NOT NULL,
                        weather_desc VARCHAR(150) NOT NULL
                       );
                       """)
        
        cursor.execute("""
                       CREATE TABLE IF NOT EXISTS core.fact_weather(
                       date_time TIMESTAMP,
                       sunrise TIMESTAMP,
                       sunset TIMESTAMP,
                       city_id INTEGER REFERENCES core.dim_city(city_id),
                       visibility_meters INTEGER,
                       cloudiness_perc FLOAT,
                       weather_con_id INTEGER REFERENCES core.dim_weather_desc(weather_id),
                       temp_c FLOAT,
                       feels_like_c FLOAT,
                       temp_max_c FLOAT,
                       temp_min_c FLOAT,
                       pressure_hpa NUMERIC(7,2),
                       sea_level_hpa NUMERIC(7,2),
                       grnd_level_hpa NUMERIC(7,2),
                       humidity_perc FLOAT,
                       wind_speed_m_s FLOAT,
                       wind_gust_m_s FLOAT,
                       wind_dir_deg NUMERIC(5,1)
                       );
                       """)
        conn.close() 

def create_staging_weather_tables():

    hook= PostgresHook(postgres_conn_id='postgres_weather_db')
    conn= hook.get_conn()
    conn.autocommit = True

    with conn.cursor() as cursor:
        cursor.execute("""
                       CREATE TABLE IF NOT EXISTS staging.city (
                       city_id INTEGER PRIMARY KEY,
                       city_name VARCHAR(30) NOT NULL,
                       lat_city DOUBLE PRECISION NOT NULL,
                       lon_city DOUBLE PRECISION NOT NULL,
                       country_code VARCHAR(5) NOT NULL
                       );
                       """)
        
        cursor.execute("""
                        CREATE TABLE IF NOT EXISTS staging.weather_desc(
                        weather_id INTEGER PRIMARY KEY,
                        weather_main VARCHAR(30) NOT NULL,
                        weather_desc VARCHAR(150) NOT NULL
                       );
                       """)
        
        cursor.execute("""
                        CREATE TABLE IF NOT EXISTS staging.weather(
                       date_time TIMESTAMP,
                       sunrise TIMESTAMP,
                       sunset TIMESTAMP,
                       city_id INTEGER REFERENCES staging.city(city_id),
                       visibility_meters INTEGER,
                       cloudiness_perc FLOAT,
                       weather_con_id INTEGER REFERENCES staging.weather_desc(weather_id),
                       temp_c FLOAT,
                       feels_like_c FLOAT,
                       temp_max_c FLOAT,
                       temp_min_c FLOAT,
                       pressure_hpa NUMERIC(7,2),
                       sea_level_hpa NUMERIC(7,2),
                       grnd_level_hpa NUMERIC(7,2),
                       humidity_perc FLOAT,
                       wind_speed_m_s FLOAT,
                       wind_gust_m_s FLOAT,
                       wind_dir_deg NUMERIC(5,1)
                       );
                       """)
        conn.close() 