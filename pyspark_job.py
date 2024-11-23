import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, decode, expr
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    ArrayType,
    LongType,
    TimestampType,
)
import logging


class KafkaCDCProcessor:
    def __init__(self, brokers: str, topic: str):
        self.brokers = brokers
        self.topic = topic
        self.spark = self._initialize_spark()
        self.appointment_schema = self._define_appointment_schema()
        self.debezium_schema = self._define_debezium_schema()

    def _initialize_spark(self) -> SparkSession:
        packages = [
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2",
            "org.apache.spark:spark-streaming-kafka-0-10_2.12:3.1.2",
            "org.apache.kafka:kafka-clients:2.1.1",
        ]

        return (
            SparkSession.builder.master(os.environ.get("SPARK_MASTER_URL", "local[*]"))
            .appName("kafka-cdc-processor")
            .config("spark.jars.packages", ",".join(packages))
            .config("spark.sql.streaming.checkpointLocation", "/tmp/checkpoint")
            .getOrCreate()
        )

    def _define_appointment_schema(self) -> StructType:
        """Define schema for the appointment data."""
        return StructType(
            [
                StructField("appointment_id", StringType(), True),
                StructField("patient_id", StringType(), True),
                StructField("patient_name", StringType(), True),
                StructField("gender", StringType(), True),
                StructField("age", IntegerType(), True),
                StructField("address", StringType(), True),
                StructField("symptoms", ArrayType(StringType()), True),
                StructField("diagnosis", StringType(), True),
                StructField("treatment_plan", StringType(), True),
                StructField("appointment_date", LongType(), True),
                StructField("doctor_name", StringType(), True),
                StructField("hospital", StringType(), True),
                StructField("insurance_provider", StringType(), True),
                StructField("payment_method", StringType(), True),
            ]
        )

    def _define_debezium_schema(self) -> StructType:
        """Define schema for the Debezium CDC format."""
        return StructType(
            [
                StructField("before", self.appointment_schema, True),
                StructField("after", self.appointment_schema, True),
                StructField(
                    "source",
                    StructType(
                        [
                            StructField("version", StringType(), True),
                            StructField("connector", StringType(), True),
                            StructField("name", StringType(), True),
                            StructField("ts_ms", LongType(), True),
                            StructField("snapshot", StringType(), True),
                            StructField("db", StringType(), True),
                            StructField("sequence", StringType(), True),
                            StructField("schema", StringType(), True),
                            StructField("table", StringType(), True),
                            StructField("txId", LongType(), True),
                            StructField("lsn", LongType(), True),
                            StructField("xmin", StringType(), True),
                        ]
                    ),
                    True,
                ),
                StructField("op", StringType(), True),
                StructField("ts_ms", LongType(), True),
                StructField("transaction", StringType(), True),
            ]
        )

    def process_stream(self):
        """Process the CDC stream from Kafka."""
        try:
            # Read from Kafka
            raw_df = (
                self.spark.readStream.format("kafka")
                .option("kafka.bootstrap.servers", self.brokers)
                .option("subscribe", self.topic)
                .option("startingOffsets", "earliest")
                .load()
            )

            # Parse the Debezium format
            parsed_df = (
                raw_df.select(decode(col("value"), "UTF-8").alias("json_data"))
                .select(from_json("json_data", self.debezium_schema).alias("data"))
                .select(
                    col("data.after.*"),
                    col("data.op").alias("operation"),
                    col("data.ts_ms").alias("cdc_timestamp"),
                    col("data.source.txId").alias("transaction_id"),
                    col("data.source.lsn").alias("log_sequence_number"),
                )
                .withColumn(
                    "appointment_date",
                    (col("appointment_date") / 1000).cast(TimestampType()),
                )
                .withColumn(
                    "cdc_timestamp", (col("cdc_timestamp")).cast(TimestampType())
                )
            )

            # Write the processed data
            query = (
                parsed_df.writeStream.outputMode("append")
                .format("console")
                .option("truncate", False)
                .start()
            )

            query.awaitTermination()

        except Exception as e:
            logging.error(f"Error processing stream: {str(e)}")
            raise


def main():
    logging.basicConfig(level=logging.INFO)

    # Configuration
    BROKERS = "broker-debezium:9094"
    TOPIC = "cdc.public.appointments"

    try:
        processor = KafkaCDCProcessor(BROKERS, TOPIC)
        processor.process_stream()
    except Exception as e:
        logging.error(f"Application failed: {str(e)}")
        raise


if __name__ == "__main__":
    main()
