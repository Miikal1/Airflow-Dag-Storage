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
    cur = conn.cursor()
    cur.execute("USE WAREHOUSE cat_query_wh")  # replace with your warehouse
    return cur

@task
def make_format():
    con = return_snowflake_conn()
    create_format_sql = f"""CREATE OR REPLACE FILE FORMAT city_weather
        TYPE = 'CSV'
        FIELD_OPTIONALLY_ENCLOSED_BY = '"'
        SKIP_HEADER = 1
        FIELD_DELIMITER = ','
        NULL_IF = ('NULL', '')
        DATE_FORMAT = 'MM/DD/YY';"""
    con.execute(create_format_sql)
    con.close()

@task
def train(train_input_table, train_view, model_name):
    con = return_snowflake_conn()
    create_view_sql = f"""CREATE OR REPLACE VIEW {train_view} AS 
        (SELECT date as ds, temp_max, city
         FROM {train_input_table});"""
    
    create_model_sql = f"""CREATE OR REPLACE SNOWFLAKE.ML.FORECAST {model_name} (
        INPUT_DATA => SYSTEM$REFERENCE('VIEW', '{train_view}'),
        SERIES_COLNAME => 'city',
        TIMESTAMP_COLNAME => 'ds',
        TARGET_COLNAME => 'temp_max',
        CONFIG_OBJECT => {{ 'ON_ERROR': 'SKIP' }}
    );"""
    
    con.execute(create_view_sql)
    con.execute(create_model_sql)
    con.close()
    
@task
def predict(model_name, forcast_table):
    con = return_snowflake_conn()
    make_prediction_sql = f"""BEGIN
        CALL {model_name}!FORECAST(
            FORECASTING_PERIODS => 7,
            -- Here we set the prediction interval.
            CONFIG_OBJECT => {{'prediction_interval': 0.95}}
        );
        -- These steps store the predictions to a table.
        LET x := SQLID;
        CREATE OR REPLACE TABLE {forcast_table} AS SELECT * FROM TABLE(RESULT_SCAN(:x));
    END;"""
    con.execute(make_prediction_sql)
    con.close()
    
  
@task
def present(train_input_table, forecast_table):
    con = return_snowflake_conn()
    presentation = f"""SELECT city, date as ds, temp_max as actual, NULL as forecast, NULL as lower_bound, NULL as upper_bound
        FROM {train_input_table}
        UNION
        SELECT "SERIES" AS city, ts as ds, NULL AS actual, forecast, lower_bound, upper_bound
        FROM {forecast_table}
        ORDER BY ds DESC;"""
    con.execute(presentation)
    con.close()
    
with DAG(
    dag_id = 'ML_forecast',
    start_date = datetime(2026,2,23),
    catchup=False,
    tags=['ML', 'ETL'],
    schedule = '30 7 * * *'
) as dag:
    train_input_table = "user_db_cat.raw.weather_data_lab1"
    train_view = "user_db_cat.raw.weather_data_view_lab1"
    model_name = "user_db_cat.analytics.forcast_model_lab1"
    forecast_table = "user_db_cat.analytics.forcast_data_lab1"
    
    
    #make_format()
    t1 = train(train_input_table, train_view, model_name)
    t2 = predict(model_name, forecast_table)
    t3 = present(train_input_table, forecast_table)
    
    t1 >> t2 >> t3