FROM apache/airflow:2.5.0-python3.9

RUN pip install --no-cache-dir \
    pandas \
    requests \
    beautifulsoup4 \
    pymssql \
    pyspark \
    apache-airflow-providers-microsoft-mssql

WORKDIR /opt/airflow
COPY dags /opt/airflow/dags
COPY scripts /opt/airflow/scripts

ENV AIRFLOW_HOME=/opt/airflow

ENTRYPOINT ["airflow"]
CMD ["webserver"]
