from datetime import datetime

from airflow import DAG
from airflow.decorators import task, task_group
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

import georisques.download as georisques_download
from air_quality.download import download_stations_details, download_daily_reports
from water_api.download import get_analysepc_filtered_year

with DAG(
    dag_id='ingest',
    start_date=datetime.now(),
    schedule=None # None means manually triggered
) as dag:

    start = EmptyOperator(task_id="start_ingestion")

    @task_group(group_id="air_quality_ingestion")
    def air_quality_ingestion():
        get_air_quality_stations_details = PythonOperator(task_id="get_air_quality_stations_details",
                                                          python_callable=download_stations_details
                                                          )

        get_air_quality_measures = PythonOperator(task_id="get_air_quality_measures",
                                                  python_callable=download_daily_reports,
                                                  op_kwargs={'years':[2021, 2022, 2023]})

        get_air_quality_stations_details >> get_air_quality_measures

    @task
    def georisques():
        georisques_download.download_data()

    @task_group(group_id="water_api_ingestion")
    def water_api_ingestion():
        PythonOperator(task_id="get_water_api_data",
                       python_callable=get_analysepc_filtered_year,
                       op_kwargs={'year': 2021, 'chemical_components': "1319,1350,1383,1386"})

    end = EmptyOperator(task_id="ingestion_finished")

    start >> georisques() >> end
    start >> air_quality_ingestion() >> end
    start >> water_api_ingestion() >> end
