from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

from datetime import timedelta
from datetime import datetime
#import snowflake.connector
import requests
import pandas as pd


def return_snowflake_conn():
    # Initialize the SnowflakeHook
    hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
    
    # Execute the query and fetch results
    conn = hook.get_conn()
    return conn.cursor()

#The Extract Method
@task
def get_past_60_days_weather(LATITUDE, LONGITUDE, CITY):
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
        "city": CITY,
        "temp_max": data["daily"]["temperature_2m_max"],
        "temp_min": data["daily"]["temperature_2m_min"],
        "precipitation": data["daily"]["precipitation_sum"],
        "weather_code": data["daily"]["weather_code"]
    })

    df["date"] = pd.to_datetime(df["date"])

    return df

@task
def combine_data(dataOne, dataTwo):
    return pd.concat([dataOne, dataTwo], ignore_index=True)

#the Transform method
@task
def transform(stuff):
    records = []
    for _, row in stuff.iterrows():
        records.append([
          float(row["latitude"]),
          float(row["longitude"]),
          row["date"].strftime("%Y-%m-%d"),  # convert datetime to string
          str(row["city"]),
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
    target_table = "user_db_cat.raw.weather_data_lab1"
    try:
        con.execute(f"""
        CREATE OR REPLACE TABLE {target_table} (
        latitude DECIMAL(6,2),
        
        longitude DECIMAL(6,2),
        date DATE,
        city VARCHAR(12),
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
            city = r[3]
            temp_max = r[4]
            temp_min = r[5]
            precipitation = r[6]
            weather_code = r[7]
            sql = f"INSERT INTO {target_table} (latitude, longitude, date, city, temp_max, temp_min, precipitation, weather_code) VALUES ('{latitude}', '{longitude}', '{date}', '{city}', '{temp_max}', '{temp_min}', '{precipitation}', '{weather_code}')"
            # print(sql)
            con.execute(sql)
        con.execute("COMMIT;")
    except Exception as e:
        con.execute("ROLLBACK;")
        print(e)
        raise e

    
with DAG(
    dag_id = 'Open_Meteo',
    start_date = datetime(2026,2,23),
    catchup=False,
    tags=['ETL'],
    schedule = '30 7 * * *'
) as dag:
    latis = Variable.get("LATISAN")
    longis = Variable.get("LONGSAN")
    latit = Variable.get("LATITAH")
    longit = Variable.get("LONGTAH")
    san = Variable.get("SANJOSE")
    tah = Variable.get("LAKETAHOE")    
    
    con = return_snowflake_conn()
    
    datas = get_past_60_days_weather(latis, longis, san)
    datat = get_past_60_days_weather(latit, longit, tah)
    data = combine_data(datas, datat)
    lines = transform(data)
    load(con, lines)