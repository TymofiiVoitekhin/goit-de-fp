"""Етап 2 (Частина 2): очищення текстових колонок, дедуплікація та збереження у шар Silver."""

import os
import re
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType

DATALAKE_DIR = os.getenv("DATALAKE_DIR", "/opt/airflow/datalake")
TABLES = ["athlete_bio", "athlete_event_results"]

# Колонки числових вимірювань виключено з regex-чистки, щоб уникнути склеювання діапазонів (наприклад, "58-62" -> "5862")
EXCLUDED_FROM_TEXT_CLEAN = {"height", "weight"}


def clean_text(text):
    """Очищення тексту від спецсимволів зі збереженням NULL-значень."""
    if text is None:
        return None
    return re.sub(r'[^a-zA-Z0-9,.\"\']', '', str(text))


def main() -> None:
    bronze_base_dir = os.path.join(DATALAKE_DIR, "bronze")
    silver_base_dir = os.path.join(DATALAKE_DIR, "silver")

    spark = (
        SparkSession.builder
        .appName("BronzeToSilver")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    clean_text_udf = udf(clean_text, StringType())

    try:
        for table in TABLES:
            bronze_path = os.path.join(bronze_base_dir, table)
            silver_path = os.path.join(silver_base_dir, table)

            # Читання даних із шару Bronze
            df = spark.read.parquet(bronze_path)
            initial_count = df.count()

            # Застосування функції чистки тексту до всіх текстових колонок
            text_columns = [
                field.name
                for field in df.schema.fields
                if isinstance(field.dataType, StringType) and field.name not in EXCLUDED_FROM_TEXT_CLEAN
            ]
            for col_name in text_columns:
                df = df.withColumn(col_name, clean_text_udf(col(col_name)))

            # Дедуплікація рядків
            df = df.dropDuplicates()
            final_count = df.count()

            # Запис очищеної таблиці у шар Silver
            df.write.mode("overwrite").parquet(silver_path)

            print(
                f"Таблицю '{table}' збережено у шар Silver: {silver_path} "
                f"(рядків до дедуплікації: {initial_count}, після: {final_count})"
            )
            df.show(10, truncate=True)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
