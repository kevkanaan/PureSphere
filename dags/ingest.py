from datetime import datetime

from airflow import DAG
from airflow.decorators import task
from airflow.operators.empty import EmptyOperator

import georisques.download as georisques_download

with DAG(
    dag_id='ingest',
    start_date=datetime.now(),
    schedule=None # None means manually triggered
) as dag:

    start = EmptyOperator(task_id="start")

    @task
    def georisques():
        print(georisques_download.get_name())

    start >> georisques()
