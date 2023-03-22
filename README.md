# weather-data-pipeline-orchestration-with-airflow

## Description 
This an end to end batch processing pipeline that puls data from the weather api into a postgres database. Airflow is used to schedule hourly data load from the api into postgesDB. Docker is used to setup the airflow and postgres infrastructure.

## Running the setup
1. Start the docker containers.
  'docker-compose up`
 2. Once the containers are running, go to the the airflow ui and trigger the `hourly_weather` dag.
 
 once the dag is triggered the end to end batch process will be ran every hour.
 
 ## DAG
 The dag contains 5 flows
 - create_table - This creates a postgres table, this makes use of the `PostgresOperator`.
 - is_api_available: This uses the HttpSensor to check if the weather api is available.
 - extract-weather: This uses the `SimpleHttpOperator`, extracts the data and stores the data in a response filter variable.
 - process_weather: This uses the PythonOperator and a python function that processes the extracted data and stores the transformed data in a temp storage.
 - store_weather: this stores the transformed data into the postgres db.


## Connections
- postgres_local: Created a postgeres local connection in the ariflow ui.
- weath_api:Create a weath-api connection in the airflow ui.
