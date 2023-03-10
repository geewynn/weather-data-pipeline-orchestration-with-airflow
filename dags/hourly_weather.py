import json
from airflow import DAG
from datetime import datetime
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.sensors.http_sensor import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from pandas import json_normalize
import datetime as dt



def _process_weather(ti):
    weather = ti.xcom_pull(task_ids='extract_weather')
    processed_weather = json_normalize(weather)
    processed_weather['timestamp'] = processed_weather['location.localtime_epoch'].apply(
        lambda s: dt.datetime.fromtimestamp(s).strftime('%Y-%m-%dT%H:%M:%S+02:00'))

    # rename the columns to have simpler names
    processed_weather.rename(columns={'location.name': 'location',
                                      'location.region': 'region',
                                      'current.temp_c': 'temp_c',
                                      'current.wind_kph': 'wind_kph'
                                      }, inplace=True)

    # filter out only the columns that we need
    processed_weather = processed_weather.filter(
        ['location', 'temp_c', 'wind_kph', 'timestamp'])

    # save to temporary csv
    processed_weather.to_csv('/tmp/processed_user.csv',
                             index=None, header=False)


def _store_weather():
    hook = PostgresHook(postgres_conn_id='postgres_local')
    hook.copy_expert(
        sql="COPY weather_new FROM stdin WITH DELIMITER as ','",
        filename='/tmp/processed_user.csv',
    )


with DAG(
    "weather_processing_two",
    start_date=datetime(2023, 3, 10),
    schedule_interval="0 * * * *",
        catchup=False) as dag:

    create_table = PostgresOperator(
        task_id='create_table',
        postgres_conn_id='postgres_local',
        sql=
        '''
            CREATE TABLE IF NOT EXISTS weather_new (
            location text not null,
            temp_c numeric not null,
            wind_kph numeric not null,
            time text not null
        );
        '''
    )

    is_api_avaliable = HttpSensor(
        task_id='is_api_avaliable',
        http_conn_id='weath_api',
        endpoint=""
    )

    extract_weather = SimpleHttpOperator(
        task_id='extract_weather',
        http_conn_id='weath_api',
        endpoint='v1/current.json?q=Kaduna',
        headers={"Content-Type": "application/json"},
        method='GET',
        response_filter=lambda response: json.loads(response.text),
        log_response=True
    )

    process_weather = PythonOperator(
        task_id='process_weather',
        python_callable=_process_weather
    )

    store_weather = PythonOperator(
        task_id='store_weather',
        python_callable=_store_weather
    )

    create_table >> is_api_avaliable >> extract_weather >> process_weather >> store_weather
