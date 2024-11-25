import os
import logging
import pandas as pd
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, decode, from_unixtime
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    LongType,
    TimestampType,
    ArrayType,
)
from snowflake.snowpark import Session
from snowflake.snowpark.functions import col as snowpark_col

load_dotenv(dotenv_path="/opt/spark/.env")


class KafkaCDCProcessor:
    def __init__(self, brokers: str, topic: str, snowflake_config: dict):
        self.brokers = brokers
        self.topic = topic
        self.snowflake_config = {
            "account": "de17043.europe-west3.gcp",
            "user": snowflake_config["sfUser"],
            "password": snowflake_config["sfPassword"],
            "role": snowflake_config["sfRole"],
            "database": snowflake_config["sfDatabase"],
            "schema": snowflake_config["sfSchema"],
            "warehouse": snowflake_config["sfWarehouse"],
        }
        self.spark = self._initialize_spark()
        self.appointment_schema = self._define_appointment_schema()
        self.debezium_schema = self._define_debezium_schema()
        self.snowflake_session = None

    def _initialize_spark(self) -> SparkSession:
        packages = [
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2",
            "org.apache.spark:spark-streaming-kafka-0-10_2.12:3.1.2",
            "org.apache.kafka:kafka-clients:2.1.1",
        ]

        return (
            SparkSession.builder.master(os.environ.get("SPARK_MASTER_URL"))
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

    def _initialize_snowflake_session(self):
        """Initialize Snowflake session using Snowpark."""
        try:
            logging.info("Attempting to connect to Snowflake...")
            return Session.builder.configs(self.snowflake_config).create()
        except Exception as e:
            logging.error(f"Failed to initialize Snowflake session: {str(e)}")
            logging.error(f"Snowflake config (masked): {self._get_masked_config()}")
            raise

    def _get_masked_config(self):
        """Return a masked version of the Snowflake config for logging."""
        masked_config = self.snowflake_config.copy()
        if "password" in masked_config:
            masked_config["password"] = "****"
        return masked_config

    def process_batch(self, df, epoch_id):
        """Process each batch of streaming data."""
        try:
            if not self.snowflake_session:
                self.snowflake_session = self._initialize_snowflake_session()

            timestamp_columns = ["appointment_date", "cdc_timestamp"]
            for col_name in timestamp_columns:
                if col_name in df.columns:
                    df = df.withColumn(
                        col_name,
                        col(col_name).cast("timestamp").cast("long"),
                    )

            pandas_df = df.toPandas()

            for col_name in timestamp_columns:
                if col_name in pandas_df.columns:
                    pandas_df[col_name] = pandas_df[col_name].apply(
                        pd.to_datetime, errors="coerce", unit="s"
                    )

            pandas_df = pandas_df[
                [
                    "appointment_id",
                    "patient_id",
                    "patient_name",
                    "gender",
                    "age",
                    "address",
                    "symptoms",
                    "diagnosis",
                    "treatment_plan",
                    "appointment_date",
                    "doctor_name",
                    "hospital",
                    "insurance_provider",
                    "payment_method",
                ]
            ]

            if not pandas_df.empty:
                logging.info(
                    f"Processing batch {epoch_id} with {len(pandas_df)} records"
                )

                snowpark_df = self.snowflake_session.create_dataframe(pandas_df)

                snowpark_df.write.mode("append").save_as_table("ALL_APPOINTMENTS")

                logging.info(f"Successfully processed batch {epoch_id}")
            else:
                logging.info(f"Batch {epoch_id} is empty, skipping")

        except Exception as e:
            logging.error(f"Error processing batch {epoch_id}: {str(e)}")
            raise

    def process_stream(self):
        """Process the CDC stream from Kafka and insert into Snowflake."""
        try:
            raw_df = (
                self.spark.readStream.format("kafka")
                .option("kafka.bootstrap.servers", self.brokers)
                .option("subscribe", self.topic)
                .option("startingOffsets", "earliest")
                .load()
            )

            # Parse the Debezium format with explicit timestamp handling
            parsed_df = (
                raw_df.select(decode(col("value"), "UTF-8").alias("json_data"))
                .select(from_json("json_data", self.debezium_schema).alias("data"))
                .select(
                    col("data.after.appointment_id"),
                    col("data.after.patient_id"),
                    col("data.after.patient_name"),
                    col("data.after.gender"),
                    col("data.after.age"),
                    col("data.after.address"),
                    col("data.after.symptoms"),
                    col("data.after.diagnosis"),
                    col("data.after.treatment_plan"),
                    col("data.after.appointment_date"),
                    col("data.after.doctor_name"),
                    col("data.after.hospital"),
                    col("data.after.insurance_provider"),
                    col("data.after.payment_method"),
                )
                .withColumn(
                    "appointment_date",
                    from_unixtime(
                        col("appointment_date").cast("long")
                        / 1000000,  # Convert microseconds to seconds
                        "yyyy-MM-dd HH:mm:ss",
                    ).cast("timestamp"),
                )
            )

            query = (
                parsed_df.writeStream.foreachBatch(self.process_batch)
                .outputMode("update")
                .option("checkpointLocation", "/tmp/checkpoint")
                .start()
            )

            console_query = (
                parsed_df.writeStream.outputMode("append")
                .format("console")
                .option("truncate", False)
                .start()
            )

            query.awaitTermination()
            console_query.awaitTermination()

        except Exception as e:
            logging.error(f"Error processing stream: {str(e)}")
            raise

    def cleanup(self):
        """Cleanup resources."""
        if self.snowflake_session:
            try:
                self.snowflake_session.close()
            except Exception as e:
                logging.error(f"Error closing Snowflake session: {str(e)}")


def main():
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s:%(message)s"
    )

    BROKERS = "broker-debezium:9094"
    TOPIC = "cdc.public.appointments"

    SNOWFLAKE_CONFIG = {
        "sfURL": f"{os.environ.get('SNOWFLAKE_ACCOUNT')}.snowflakecomputing.com",
        "sfUser": os.environ.get("SNOWFLAKE_USER"),
        "sfPassword": os.environ.get("SNOWFLAKE_PASSWORD"),
        "sfRole": os.environ.get("SNOWFLAKE_ROLE"),
        "sfDatabase": os.environ.get("SNOWFLAKE_DATABASE"),
        "sfSchema": os.environ.get("SNOWFLAKE_SCHEMA"),
        "sfWarehouse": os.environ.get("SNOWFLAKE_WAREHOUSE"),
    }

    try:
        processor = KafkaCDCProcessor(BROKERS, TOPIC, SNOWFLAKE_CONFIG)
        processor.process_stream()
    except Exception as e:
        logging.error(f"Application failed: {str(e)}")
        raise


if __name__ == "__main__":
    main()
