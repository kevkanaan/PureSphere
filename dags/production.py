from datetime import datetime

from airflow import DAG
from airflow.decorators import task, task_group
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from graph_db.create_graph import create_graph_db, identify_monitoring_stations_for_industrial_site, create_monitoring_stations_sql_table

with DAG(
    dag_id='production',
    start_date=datetime.now(),
    schedule=None # None means manually triggered
) as dag:

    start = EmptyOperator(task_id="start_production")

    create_stations_sql_table = PythonOperator(task_id="create_monitoring_stations_sql_table",
                                                                    python_callable=create_monitoring_stations_sql_table,
                                                                    trigger_rule="all_success")
    
    match_industrial_site_to_monitoring_stations = PythonOperator(task_id="match_industrial_site_to_monitoring_stations",
                                                                    python_callable=identify_monitoring_stations_for_industrial_site,
                                                                    trigger_rule="all_success")

    create_graph_database = PythonOperator(task_id="create_graph_database",
                                            python_callable=create_graph_db,
                                            trigger_rule="all_success")

    end = EmptyOperator(task_id="production_finished")

    start >> create_stations_sql_table >> match_industrial_site_to_monitoring_stations >> create_graph_database >> end
