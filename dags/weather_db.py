#--------------------
# DAG Name: weather_data_db
# Author:             Dániel Varga
# Created:            2025-08-10
# Last Modified:      2025-08-013
#
#This DAG creates the PostgreSQL database structure for storing weather data.
#Steps:
#1. Create the PostgreSQL database (if it does not exist).
#2. Create the required schemas (`staging` and `core`).
#3. Create the staging tables for raw imported weather data.
#4. Create the core tables for the star-schema dimensional model.
#
#Schedule: Daily at 05:00 CET (mainly for maintenance/recreation in dev environments).
#--------------------

from datetime import datetime

from airflow.sdk import dag, task
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

from src.create_weather_db import create_weather_pg_db
from src.create_weather_db import create_staging_weather_tables
from src.create_weather_db import create_core_weather_tables


@dag(
    start_date=datetime(2025,8,9),
    schedule='0 5 * * *',
    catchup=False,
    tags=['weather','SQL','postgres']
    )
def weather_data_db():
    """
    Airflow DAG to set up the PostgreSQL database, schemas and table structure
    for the weather ETL pipeline.
    """
    
    # Task to create main PostgreSQL database if it does not exist.
    @task
    def create_db():
        create_weather_pg_db()


    # Task to create staging schema for temporary/raw data
    create_schema_staging= SQLExecuteQueryOperator(
        task_id='create_schema_staging',
        conn_id='postgres_weather_db',
        sql="""
            CREATE SCHEMA IF NOT EXISTS staging;
            """,
    )

    # Create core schema
    create_schema_core= SQLExecuteQueryOperator(
        task_id='create_schema_core',
        conn_id='postgres_weather_db',
        sql="""
            CREATE SCHEMA IF NOT EXISTS core;
            """,
    )


    # Task to create staging tables for raw imported weather data.
    @task
    def create_staging_tables():
        create_staging_weather_tables()

    # Task to create core tables for the star-schema dimensional model.
    @task
    def create_core_tables():
        create_core_weather_tables()
    
    # Task dependencies to ensure the correct order of execution.
    t1=create_db()
    t4=create_staging_tables()
    t5=create_core_tables()


    t1 >> [create_schema_staging, create_schema_core]
    create_schema_staging >> t4
    create_schema_core >> t5
    

weather_data_db()