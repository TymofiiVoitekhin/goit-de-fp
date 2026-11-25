"""Підготовчий скрипт (Частина 1): наповнення локальної MySQL БД olympic_dataset даними з CSV."""

import os
import requests
from pyspark.sql import SparkSession

BASE_URL = "https://ftp.goit.study/neoversity/"
DATALAKE_DIR = os.getenv("DATALAKE_DIR", "/opt/airflow/datalake")
JDBC_JAR_PATH = "/opt/airflow/jars/mysql-connector-j-8.0.32.jar"
TABLES = ["athlete_bio", "athlete_event_results"]


def ensure_csv_exists(table_name: str, landing_dir: str) -> str:
    """Перевірка наявності CSV у landing; за відсутності — завантаження з сервера курсу."""
    os.makedirs(landing_dir, exist_ok=True)
    local_file_path = os.path.join(landing_dir, f"{table_name}.csv")

    if os.path.exists(local_file_path):
        print(f"Використано локальний файл: {local_file_path}")
        return local_file_path

    downloading_url = f"{BASE_URL}{table_name}.csv"
    print(f"Завантаження файлу: {downloading_url}")
    response = requests.get(downloading_url, timeout=60)
    response.raise_for_status()

    with open(local_file_path, "wb") as file:
        file.write(response.content)

    print(f"Файл збережено: {local_file_path}")
    return local_file_path


def main() -> None:
    mysql_host = os.getenv("MYSQL_HOST", "mysql")
    mysql_port = os.getenv("MYSQL_PORT", "3306")
    mysql_db = os.getenv("MYSQL_DATABASE", "olympic_dataset")
    mysql_user = os.getenv("MYSQL_USER")
    mysql_password = os.getenv("MYSQL_PASSWORD")

    if not mysql_user or not mysql_password:
        raise ValueError("Не задано змінні оточення MYSQL_USER або MYSQL_PASSWORD.")

    jdbc_url = (
        f"jdbc:mysql://{mysql_host}:{mysql_port}/{mysql_db}"
        "?rewriteBatchedStatements=true"
    )
    landing_dir = os.path.join(DATALAKE_DIR, "landing")

    spark = (
        SparkSession.builder
        .appName("InitLocalMySQL")
        .config("spark.jars", JDBC_JAR_PATH)
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    try:
        for table in TABLES:
            csv_path = ensure_csv_exists(table, landing_dir)

            df = (
                spark.read
                .option("header", "true")
                .option("inferSchema", "false")
                .option("quote", '"')
                .option("escape", '"')
                .csv(csv_path)
            )

            # Запис таблиці у локальний MySQL через JDBC пакетами по 10 000 рядків
            (
                df.write
                .format("jdbc")
                .option("url", jdbc_url)
                .option("driver", "com.mysql.cj.jdbc.Driver")
                .option("dbtable", table)
                .option("user", mysql_user)
                .option("password", mysql_password)
                .option("batchsize", "10000")
                .mode("overwrite")
                .save()
            )

            print(f"Таблицю '{mysql_db}.{table}' завантажено у MySQL ({df.count()} рядків).")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
