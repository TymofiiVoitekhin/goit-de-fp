"""Етап 1 (Частина 2): завантаження CSV із FTP-сервера та збереження у шар Bronze (Parquet)."""

import os
import requests
from pyspark.sql import SparkSession

BASE_URL = "https://ftp.goit.study/neoversity/"
DATALAKE_DIR = os.getenv("DATALAKE_DIR", "/opt/airflow/datalake")
TABLES = ["athlete_bio", "athlete_event_results"]


def download_data(table_name: str, landing_dir: str) -> str:
    """Завантаження CSV-файлу з віддаленого сервера у директорію landing."""
    os.makedirs(landing_dir, exist_ok=True)
    downloading_url = f"{BASE_URL}{table_name}.csv"
    local_file_path = os.path.join(landing_dir, f"{table_name}.csv")

    print(f"Завантаження файлу: {downloading_url}")
    response = requests.get(downloading_url, timeout=60)
    response.raise_for_status()

    with open(local_file_path, "wb") as file:
        file.write(response.content)

    print(f"Файл збережено: {local_file_path}")
    return local_file_path


def main() -> None:
    landing_dir = os.path.join(DATALAKE_DIR, "landing")
    bronze_base_dir = os.path.join(DATALAKE_DIR, "bronze")

    spark = (
        SparkSession.builder
        .appName("LandingToBronze")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    try:
        for table in TABLES:
            # Завантаження сирого CSV-файлу у зону landing
            csv_path = download_data(table, landing_dir)

            # Читання CSV без приведення типів для збереження оригінального вигляду даних у Bronze
            df = (
                spark.read
                .option("header", "true")
                .option("inferSchema", "false")
                .option("quote", '"')
                .option("escape", '"')
                .csv(csv_path)
            )

            # Запис таблиці у форматі Parquet у директорію bronze/{table}
            bronze_path = os.path.join(bronze_base_dir, table)
            df.write.mode("overwrite").parquet(bronze_path)

            print(f"Таблицю '{table}' збережено у шар Bronze: {bronze_path}")
            print(f"Кількість рядків ({table}): {df.count()}")
            df.printSchema()
            df.show(10, truncate=True)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()