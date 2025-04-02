#!/bin/bash

#pip install minio

airflow db init

airflow users create \
    --username ${_AIRFLOW_WWW_USER_USERNAME} \
    --password ${_AIRFLOW_WWW_USER_PASSWORD} \
    --firstname Gabriel \
    --lastname Admin \
    --role Admin \
    --email contato.magalhaesgabriel@gmail.com

airflow webserver & airflow scheduler