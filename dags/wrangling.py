from datetime import datetime
from enum import auto

from airflow import DAG
from airflow.decorators import task_group
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

from air_quality.wrangling import remove_invalid_measurements, drop_useless_columns, aggregate_measurement_by_site_code_and_pollutant_type, create_sql_script_air_quality_table

with DAG(
    dag_id='wrangling',
    start_date=datetime.now(),
    schedule=None, # None means manually triggered,
) as dag:

    start = EmptyOperator(task_id="start_wrangling")

    @task_group(group_id="air_quality_wrangling")
    def air_quality_wrangling():

        first_step = BashOperator(task_id="copy_stations_metadata_file",
                                 bash_command="cp -f /opt/airflow/data/landing/air-quality/stations_metadata.csv /opt/airflow/data/staging/air-quality")

        second_step = PythonOperator(task_id="remove_invalid_measurements",
                                    python_callable=remove_invalid_measurements,
                                    trigger_rule="all_success")

        third_step = PythonOperator(task_id="drop_useless_columns",
                                    python_callable=drop_useless_columns,
                                    trigger_rule="all_success")

        fourth_step = PythonOperator(task_id="aggregate_measurement_by_site_code_and_pollutant_type",
                                    python_callable=aggregate_measurement_by_site_code_and_pollutant_type,
                                    trigger_rule="all_success")

        fifth_step = SparkSubmitOperator(task_id="merge_daily_files",
                                          conn_id="spark-conn",
                                          application="/opt/airflow/jobs/air-quality/merge_daily_reports.py",
                                          trigger_rule="all_success")
        
        sixth_step = PythonOperator(task_id="create_sql_script_air_quality_table",
                                   python_callable=create_sql_script_air_quality_table,
                                   trigger_rule="all_success")
        
        seventh_step = PostgresOperator(task_id="create_air_quality_table",
                                        postgres_conn_id="postgres_conn_id",
                                        sql="air_quality/sql_scripts/create_air_quality_table.sql",
                                        trigger_rule="all_success",
                                        autocommit=True)

        first_step >> second_step >> third_step >> fourth_step >> fifth_step >> sixth_step >> seventh_step

    @task_group(group_id="georisque_wrangling")
    def georisques_wrangling():
        EmptyOperator(task_id="georisque_wrangling")

    @task_group(group_id="water_quality_wrangling")
    def water_quality_wrangling():
        EmptyOperator(task_id="water_quality_wrangling")

    end = EmptyOperator(task_id="wrangling_finished")

    start >> georisques_wrangling() >> end
    start >> air_quality_wrangling() >> end
    start >> water_quality_wrangling() >> end
