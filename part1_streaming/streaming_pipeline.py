"""Частина 1: End-to-End Streaming Pipeline (MySQL + Kafka + Spark Structured Streaming)."""

import os
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import avg, broadcast, col, current_timestamp, expr, from_json, struct, to_json
from pyspark.sql.types import StringType, StructField, StructType

JDBC_JAR_PATH = "/opt/airflow/jars/mysql-connector-j-8.0.32.jar"
CHECKPOINT_DIR = "/opt/airflow/checkpoints/athlete_streaming"

# Схема JSON-повідомлень із вхідного Kafka-топіку athlete_event_results
EVENTS_SCHEMA = StructType([
    StructField("edition", StringType(), True),
    StructField("edition_id", StringType(), True),
    StructField("country_noc", StringType(), True),
    StructField("sport", StringType(), True),
    StructField("event", StringType(), True),
    StructField("result_id", StringType(), True),
    StructField("athlete", StringType(), True),
    StructField("athlete_id", StringType(), True),
    StructField("pos", StringType(), True),
    StructField("medal", StringType(), True),
    StructField("isTeamSport", StringType(), True),
])


def main() -> None:
    mysql_host = os.getenv("MYSQL_HOST", "mysql")
    mysql_port = os.getenv("MYSQL_PORT", "3306")
    mysql_db = os.getenv("MYSQL_DATABASE", "olympic_dataset")
    mysql_user = os.getenv("MYSQL_USER")
    mysql_password = os.getenv("MYSQL_PASSWORD")
    mysql_output_table = os.getenv("MYSQL_OUTPUT_TABLE", "athlete_enriched_agg")

    kafka_bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    kafka_input_topic = os.getenv("KAFKA_INPUT_TOPIC", "athlete_event_results")
    kafka_output_topic = os.getenv("KAFKA_OUTPUT_TOPIC", "athlete_enriched_agg")

    if not mysql_user or not mysql_password:
        raise ValueError("Не задано змінні оточення MYSQL_USER або MYSQL_PASSWORD.")

    jdbc_url = (
        f"jdbc:mysql://{mysql_host}:{mysql_port}/{mysql_db}"
        "?rewriteBatchedStatements=true"
    )

    spark = (
        SparkSession.builder
        .appName("OlympicStreamingPipeline")
        .config("spark.jars", JDBC_JAR_PATH)
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    try:
        # Етап 1: зчитування даних фізичних показників атлетів із MySQL таблиці olympic_dataset.athlete_bio
        bio_df = (
            spark.read
            .format("jdbc")
            .option("url", jdbc_url)
            .option("driver", "com.mysql.cj.jdbc.Driver")
            .option("dbtable", "athlete_bio")
            .option("user", mysql_user)
            .option("password", mysql_password)
            .load()
        )

        # Етап 2: фільтрація записів із порожніми або нечисловими значеннями зросту й ваги
        bio_clean_df = (
            bio_df
            .select(
                col("athlete_id").cast("string").alias("athlete_id"),
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

        # Етап 3 (частина Б): зчитування результатів змагань із Kafka-топіку та перетворення JSON у колонки
        raw_kafka_stream = (
            spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", kafka_bootstrap)
            .option("subscribe", kafka_input_topic)
            .option("startingOffsets", "earliest")
            .load()
        )

        parsed_events_stream = (
            raw_kafka_stream
            .select(from_json(col("value").cast("string"), EVENTS_SCHEMA).alias("data"))
            .select(
                col("data.athlete_id").cast("string").alias("athlete_id"),
                col("data.sport").alias("sport"),
                col("data.medal").alias("medal"),
            )
        )

        # Етап 4: об'єднання потоку результатів змагань із біологічними даними за ключем athlete_id
        enriched_stream = parsed_events_stream.join(
            broadcast(bio_clean_df),
            on="athlete_id",
            how="inner",
        )

        # Етап 5: розрахунок середніх зросту й ваги за (sport, medal, sex, country_noc) та додавання timestamp
        aggregated_stream = (
            enriched_stream
            .groupBy("sport", "medal", "sex", "country_noc")
            .agg(
                avg("height").alias("avg_height"),
                avg("weight").alias("avg_weight"),
            )
            .withColumn("timestamp", current_timestamp())
        )

        # Етап 6: функція Fan-Out запису кожного мікробатчу у вихідний Kafka-топік та MySQL
        def foreach_batch_function(batch_df: DataFrame, batch_id: int) -> None:
            if batch_df.isEmpty():
                print(f"Мікробатч {batch_id} порожній — запис пропущено.")
                return

            batch_df.persist()
            try:
                rows_count = batch_df.count()
                print(f"Обробка мікробатчу {batch_id}: {rows_count} агрегованих рядків.")

                # Етап 6.а): запис агрегованих даних у вихідний Kafka-топік
                kafka_out_df = batch_df.select(
                    to_json(struct("*"), {"ignoreNullFields": "false"}).alias("value")
                )
                (
                    kafka_out_df.write
                    .format("kafka")
                    .option("kafka.bootstrap.servers", kafka_bootstrap)
                    .option("topic", kafka_output_topic)
                    .save()
                )
                print(f"Мікробатч {batch_id} записано у Kafka-топік '{kafka_output_topic}'.")

                # Етап 6.b): запис агрегованих даних у базу даних MySQL
                (
                    batch_df.write
                    .format("jdbc")
                    .option("url", jdbc_url)
                    .option("driver", "com.mysql.cj.jdbc.Driver")
                    .option("dbtable", mysql_output_table)
                    .option("user", mysql_user)
                    .option("password", mysql_password)
                    .option("batchsize", "5000")
                    .mode("append")
                    .save()
                )
                print(f"Мікробатч {batch_id} записано у MySQL таблицю '{mysql_db}.{mysql_output_table}'.")
            finally:
                batch_df.unpersist()

        # Запуск потокової обробки з функцією foreachBatch
        query = (
            aggregated_stream.writeStream
            .foreachBatch(foreach_batch_function)
            .outputMode("update")
            .option("checkpointLocation", CHECKPOINT_DIR)
            .trigger(availableNow=True)
            .start()
        )
        query.awaitTermination()
        print("Потокову обробку успішно завершено.")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
