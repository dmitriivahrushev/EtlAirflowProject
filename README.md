# de-start-sprint-etl-airflow-project
Запуск 
docker compose -p sprint-etl-project up -d


База данных:
services: postgres_db
POSTGRES_USER: postgres
POSTGRES_PASSWORD: postgres
POSTGRES_DB: sprint_etl_db
ports:
    - 5430:5432


airflow-webserver:
User: airflow
Pass: airflow 
ports:
    - 8070:8080