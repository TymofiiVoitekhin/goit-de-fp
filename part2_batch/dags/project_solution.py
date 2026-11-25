"""Етап 4 (Частина 2): Airflow DAG для послідовного запуску ETL-пайплайну Data Lake."""

from datetime import datetime
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 0,
}

with DAG(
    dag_id="olympic_datalake_pipeline",
    default_args=default_args,
    description="Послідовний multi-hop ETL пайплайн: Landing -> Bronze -> Silver -> Gold",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["goit", "final_project", "batch"],
) as dag:

    # Етап 1: завантаження CSV із FTP та збереження у шар Bronze (Parquet)
    landing_to_bronze = SparkSubmitOperator(
        task_id="landing_to_bronze",
        application="/opt/airflow/dags/landing_to_bronze.py",
        conn_id="spark_default",
        verbose=True,
    )

    # Етап 2: очищення тексту, дедуплікація та збереження у шар Silver
    bronze_to_silver = SparkSubmitOperator(
        task_id="bronze_to_silver",
        application="/opt/airflow/dags/bronze_to_silver.py",
        conn_id="spark_default",
        verbose=True,
    )

    # Етап 3: об'єднання таблиць, розрахунок середніх показників і збереження у шар Gold
    silver_to_gold = SparkSubmitOperator(
        task_id="silver_to_gold",
        application="/opt/airflow/dags/silver_to_gold.py",
        conn_id="spark_default",
        verbose=True,
    )

    # Послідовність виконання шарів Data Lake
    landing_to_bronze >> bronze_to_silver >> silver_to_gold
