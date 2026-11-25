"""Етап 3 (Частина 2): об'єднання таблиць Silver, агрегація показників і збереження у шар Gold."""

import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, broadcast, col, current_timestamp, expr

DATALAKE_DIR = os.getenv("DATALAKE_DIR", "/opt/airflow/datalake")


def main() -> None:
    silver_base_dir = os.path.join(DATALAKE_DIR, "silver")
    gold_output_dir = os.path.join(DATALAKE_DIR, "gold", "avg_stats")

    spark = (
        SparkSession.builder
        .appName("SilverToGold")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    try:
        # Читання очищених таблиць із шару Silver
        bio_df = spark.read.parquet(os.path.join(silver_base_dir, "athlete_bio"))
        events_df = spark.read.parquet(os.path.join(silver_base_dir, "athlete_event_results"))

        # Безпечне приведення зросту та ваги до числового типу й фільтрація невалідних записів
        bio_clean_df = (
            bio_df
            .select(
                col("athlete_id"),
                col("sex"),
                col("country_noc"),
                expr("try_cast(height as double)").alias("height"),
                expr("try_cast(weight as double)").alias("weight"),
            )
            .filter(
                col("height").isNotNull() & (col("height") > 0)
                & col("weight").isNotNull() & (col("weight") > 0)
            )
            .dropDuplicates(["athlete_id"])
        )

        # Вибір потрібних колонок із результатів змагань для уникнення колізії country_noc
        events_clean_df = events_df.select(
            col("athlete_id"),
            col("sport"),
            col("medal"),
        )

        # Об'єднання таблиць за ключем athlete_id із broadcast для довідника атлетів
        joined_df = events_clean_df.join(
            broadcast(bio_clean_df),
            on="athlete_id",
            how="inner",
        )

        # Розрахунок середніх значень зросту й ваги та додавання часової мітки
        gold_df = (
            joined_df
            .groupBy("sport", "medal", "sex", "country_noc")
            .agg(
                avg("height").alias("avg_height"),
                avg("weight").alias("avg_weight"),
            )
            .withColumn("timestamp", current_timestamp())
        )

        # Запис фінального аналітичного набору у шар Gold
        gold_df.write.mode("overwrite").parquet(gold_output_dir)

        print(f"Таблицю 'avg_stats' збережено у шар Gold: {gold_output_dir}")
        print(f"Кількість агрегованих груп: {gold_df.count()}")
        gold_df.printSchema()
        gold_df.show(20, truncate=False)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
