from datetime import datetime
from airflow.sdk import dag, task
from src.create_weather_db import create_weather_pg_db
from src.create_weather_db import create_staging_weather_tables
from src.create_weather_db import create_core_weather_tables
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator


@dag(
    start_date=datetime(2025,8,9),
    schedule='0 5 * * *',
    catchup=False,
    tags=['weather','SQL','postgres']
    )
def weather_data_db():

    @task
    def create_db():
        create_weather_pg_db()

    create_schema_staging= SQLExecuteQueryOperator(
        task_id='create_schema_staging',
        conn_id='postgres_weather_db',
        sql="""
            CREATE SCHEMA IF NOT EXISTS staging;
            """,
    )

    create_schema_core= SQLExecuteQueryOperator(
        task_id='create_schema_core',
        conn_id='postgres_weather_db',
        sql="""
            CREATE SCHEMA IF NOT EXISTS core;
            """,
    )

    @task
    def create_staging_tables():
        create_staging_weather_tables()

    @task
    def create_core_tables():
        create_core_weather_tables()
    
    t1=create_db()
    t4=create_staging_tables()
    t5=create_core_tables()


    t1 >> [create_schema_staging, create_schema_core]
    create_schema_staging >> t4
    create_schema_core >> t5
    






weather_data_db()