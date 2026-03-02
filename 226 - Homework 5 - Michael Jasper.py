from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

from datetime import timedelta
from datetime import datetime
#import snowflake.connector
import requests
import pandas as pd

# Kyoto, Japan coordinates
#LATITUDE = 35.0211
#LONGITUDE = 135.7538


#user_id = userdata.get('snowflake_userid')
#password = userdata.get('snowflake_password')
#account = userdata.get('snowflake_account')

def return_snowflake_conn():
    # Initialize the SnowflakeHook
    hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
    
    # Execute the query and fetch results
    conn = hook.get_conn()
    return conn.cursor()

#The Extract Method
@task
def get_past_60_days_weather(LATITUDE, LONGITUDE):
    """Get the past 60 days of weather Kyoto"""

    url = "https://api.open-meteo.com/v1/forecast"

    params = {
        "latitude": LATITUDE,
        "longitude": LONGITUDE,
        "past_days": 60,
        "forecast_days": 0,  # only past weather
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
        "temp_max": data["daily"]["temperature_2m_max"],
        "temp_min": data["daily"]["temperature_2m_min"],
        "precipitation": data["daily"]["precipitation_sum"],
        "weather_code": data["daily"]["weather_code"]
    })

    df["date"] = pd.to_datetime(df["date"])

    return df

#the Transform method
@task
def transform(stuff):
    records = []
    for _, row in stuff.iterrows():
        records.append([
          float(row["latitude"]),
          float(row["longitude"]),
          row["date"].strftime("%Y-%m-%d"),  # convert datetime to string
          float(row["temp_max"]),
          float(row["temp_min"]),
          float(row["precipitation"]),
          str(row["weather_code"])
      ])
    return records

#The Load Method
@task
def load(con, records):
    # full refresh
    target_table = "user_db_cat.raw.weather_kyoto"
    try:
        con.execute(f"""
        CREATE OR REPLACE TABLE {target_table} (
        latitude DECIMAL(6,2),
        longitude DECIMAL(6,2),
        date DATE,
        temp_max DECIMAL(4,2),
        temp_min DECIMAL(4,2),
        precipitation DECIMAL(4,2),
        weather_code VARCHAR(3)
        )""")
        # load records
        for r in records: # we want records except the first one
            latitude = r[0]
            longitude = r[1]
            date = r[2]
            temp_max = r[3]
            temp_min = r[4]
            precipitation = r[5]
            weather_code = r[6]
            sql = f"INSERT INTO {target_table} (latitude, longitude, date, temp_max, temp_min, precipitation, weather_code) VALUES ('{latitude}', '{longitude}', '{date}', '{temp_max}', '{temp_min}', '{precipitation}', '{weather_code}')"
            # print(sql)
            con.execute(sql)
        con.execute("COMMIT;")
    except Exception as e:
        con.execute("ROLLBACK;")
        print(e)
        raise e
    
    
with DAG(
    dag_id = 'KyotoWeather_v1',
    start_date = datetime(2026,2,23),
    catchup=False,
    tags=['ETL'],
    schedule = '30 7 * * *'
) as dag:
    lati = Variable.get("LATITUDE")
    longi = Variable.get("LONGITUDE")
    
    con = return_snowflake_conn()
    
    data = get_past_60_days_weather(lati, longi)
    lines = transform(data)
    load(con, lines)