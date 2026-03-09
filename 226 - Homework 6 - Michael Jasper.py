from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task
from airflow.operators.python import get_current_context
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

from datetime import timedelta
from datetime import datetime
import os
import requests
import pandas as pd

# Kyoto, Japan coordinates
#LATITUDE = 35.0211
#LONGITUDE = 135.7538

def get_logical_date():
    # Get the current context
    context = get_current_context()
    return str(context['logical_date'])[:10]

def get_next_day(date_str):
    # Convert the string date to a datetime object
    date_obj = datetime.strptime(date_str, "%Y-%m-%d")
    # Add one day using timedelta
    next_day = date_obj + timedelta(days=1)
    # Convert back to string in "YYYY-MM-DD" format
    return next_day.strftime("%Y-%m-%d")

def return_snowflake_conn():
    # Initialize the SnowflakeHook
    hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
    conn = hook.get_conn()
    return conn.cursor()

def get_past_1_day_weather(start_date, end_date, city, LATITUDE, LONGITUDE):
    """Get the past day of weather Kyoto"""

    url = "https://api.open-meteo.com/v1/forecast"

    params = {
        "latitude": LATITUDE,
        "longitude": LONGITUDE,
        "start_date": start_date,
        "end_date": end_date,
        "daily": [
            "temperature_2m_max",
            "temperature_2m_min",
            "precipitation_sum",
            "weather_code"
        ]
    }

    response = requests.get(url, params=params)
    data = response.json()

    # Convert data into DataFrame
    df = pd.DataFrame({
        "latitude": data["latitude"],
        "longitude": data["longitude"],
        "date": data["daily"]["time"],
        "city": city,
        "temp_max": data["daily"]["temperature_2m_max"],
        "temp_min": data["daily"]["temperature_2m_min"],
        "precipitation": data["daily"]["precipitation_sum"],
        "weather_code": data["daily"]["weather_code"]
    })

    df["date"] = pd.to_datetime(df["date"])

    return df

def save_weather_data(city, latitude, longitude, start_date, end_date, file_path):
    data = get_past_1_day_weather(start_date, end_date, city, latitude, longitude)
    data.to_csv(file_path, index=False)
    return

def populate_table_via_stage(con, database, schema, table, file_path):
    
    # Create a temporary named stage instead of using the table stage
    stage_name = f"TEMP_STAGE_{table}"
    file_name = os.path.basename(file_path) # extract only filename from the path
    
    # First set the schema since table stage or temp stage needs to have the schema as the target table
    con.execute(f"USE SCHEMA {database}.{schema}")
    # Create a temporary named stage
    con.execute(f"CREATE TEMPORARY STAGE {stage_name}")
    # Copy the given file to the temporary stage
    con.execute(f"PUT file://{file_path} @{stage_name}")
    # Run copy into command with fully qualified table name
    copy_query = f"""
    COPY INTO {schema}.{table}
    FROM @{stage_name}/{file_name}
    FILE_FORMAT = (
    TYPE = 'CSV'
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    SKIP_HEADER = 1
    )
    """
    con.execute(copy_query)
    
@task
def extract(city, latitude, longitude):
    date_to_fetch = get_logical_date()
    next_day_of_date_to_fetch = get_next_day(date_to_fetch) 
    file_path = f"/tmp/{city}_{date_to_fetch}.csv"
    
    save_weather_data(city, latitude, longitude, date_to_fetch, next_day_of_date_to_fetch, file_path)
    return file_path

#The Load Method
@task
def load(con, file_path, database, schema, target_table):
    # full refresh
    date_to_fetch = get_logical_date()
    next_day_of_date_to_fetch = get_next_day(date_to_fetch)
    print(f"========= Updating {date_to_fetch}'s data ===========")
    try:
        con.execute("BEGIN;")
        con.execute(f"""CREATE TABLE IF NOT EXISTS {database}.{schema}.{target_table} (
        latitude DECIMAL(6,2),
        longitude DECIMAL(6,2),
        weather_date DATE, 
        city varchar(12),
        temp_max DECIMAL(4,2),
        temp_min DECIMAL(4,2),
        precipitation DECIMAL(4,2),
        weather_code varchar(3)
        )""")
        con.execute(f"DELETE FROM {database}.{schema}.{target_table} WHERE weather_date='{date_to_fetch}'")
        populate_table_via_stage(con, database, schema, target_table, file_path)
        con.execute("COMMIT;")
    except Exception as e:
        con.execute("ROLLBACK;")
        print(e)
        raise e
    
    
with DAG(
    dag_id = 'KyotoWeather_Incremental',
    start_date = datetime(2026,2,28),
    catchup=False,
    tags=['ETL'],
    schedule = '30 7 * * *'
) as dag:
    lati = Variable.get("LATITUDE")
    longi = Variable.get("LONGITUDE")
    city = Variable.get("KYOTO")
    
    con = return_snowflake_conn()
    
    database = "user_db_cat"
    schema = "raw"
    target_table = "weather_kyoto_incrementing"
    
    file_path = extract(city, lati, longi)
    load(con, file_path, database, schema, target_table)
