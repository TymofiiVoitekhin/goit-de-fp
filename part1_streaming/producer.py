"""Етап 3 (частина А): зчитування athlete_event_results із MySQL та запис у вхідний Kafka-топік."""

import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, struct, to_json

JDBC_JAR_PATH = "/opt/airflow/jars/mysql-connector-j-8.0.32.jar"


def main() -> None:
    mysql_host = os.getenv("MYSQL_HOST", "mysql")
    mysql_port = os.getenv("MYSQL_PORT", "3306")
    mysql_db = os.getenv("MYSQL_DATABASE", "olympic_dataset")
    mysql_user = os.getenv("MYSQL_USER")
    mysql_password = os.getenv("MYSQL_PASSWORD")

    kafka_bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    kafka_input_topic = os.getenv("KAFKA_INPUT_TOPIC", "athlete_event_results")

    if not mysql_user or not mysql_password:
        raise ValueError("Не задано змінні оточення MYSQL_USER або MYSQL_PASSWORD.")

    jdbc_url = f"jdbc:mysql://{mysql_host}:{mysql_port}/{mysql_db}"

    spark = (
        SparkSession.builder
        .appName("MySQLToKafkaProducer")
        .config("spark.jars", JDBC_JAR_PATH)
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    try:
        # Етап 3 (частина А): зчитування даних із MySQL таблиці athlete_event_results
        events_df = (
            spark.read
            .format("jdbc")
            .option("url", jdbc_url)
            .option("driver", "com.mysql.cj.jdbc.Driver")
            .option("dbtable", "athlete_event_results")
            .option("user", mysql_user)
            .option("password", mysql_password)
            .load()
        )

        # Формування повідомлень Kafka: key = athlete_id, value = JSON із усіма полями рядка
        kafka_df = events_df.select(
            col("athlete_id").cast("string").alias("key"),
            to_json(struct("*")).alias("value"),
        )

        # Етап 3 (частина А): запис подій у вхідний Kafka-топік athlete_event_results
        (
            kafka_df.write
            .format("kafka")
            .option("kafka.bootstrap.servers", kafka_bootstrap)
            .option("topic", kafka_input_topic)
            .save()
        )

        print(
            f"Події з MySQL успішно відправлено у Kafka-топік '{kafka_input_topic}' "
            f"({events_df.count()} повідомлень)."
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
