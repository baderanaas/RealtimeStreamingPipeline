import os
import logging
from pyspark import SparkConf
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO)


def create_spark_connection():
    conf = SparkConf()
    conf.set("spark.master", "spark://spark-master:7077")

    try:
        spark_conn = (
            SparkSession.builder.appName("pyspark-job").config(conf=conf).getOrCreate()
        )
        spark_conn.sparkContext.setLogLevel("ERROR")
        logging.info("Spark session created successfully")
        return spark_conn
    except Exception as e:
        logging.error(f"Failed to create Spark session: {e}")
        return None


def get_streaming_dataframe(spark_conn):
    try:
        spark_df = (
            spark_conn.readStream.format("kafka")
            .option("kafka.bootstrap.servers", "broker-debezium:9094")
            .option("subscribe", "cdc.public.appointments")
            .option("startingOffsets", "earliest")
            .load()
        )

        logging.info("Kafka dataframe created successfully")
        return spark_df
    except Exception as e:
        logging.error(f"Failed to create Kafka dataframe: {e}")
        return None


def transform_streaming_data(df):
    schema = StructType(
        [
            StructField("appointment_id", StringType(), True),
            StructField("patient_id", StringType(), True),
            StructField("patient_name", StringType(), True),
            StructField("gender", StringType(), True),
            StructField("age", IntegerType(), True),
            StructField("address", StringType(), True),
            StructField("symptoms", StringType(), True),
            StructField("diagnosis", StringType(), True),
            StructField("treatment_plan", StringType(), True),
            StructField("appointment_date", StringType(), True),
            StructField("doctor_name", StringType(), True),
            StructField("hospital", StringType(), True),
            StructField("insurance_provider", StringType(), True),
            StructField("payment_method", StringType(), True),
        ]
    )

    sel = (
        df.selectExpr("CAST(value AS STRING)")
        .select(from_json(col("value"), schema).alias("data"))
        .select("data.*")
    )
    logging.info(f"Selection dataframe created: {sel}")

    return sel


def write_to_file(df: DataFrame, output_path: str):
    """
    Writes the processed streaming data to a file.

    :param df: The DataFrame to be written.
    :param output_path: The directory where the data will be written.
    """
    try:
        if not os.path.exists(output_path):
            os.makedirs(output_path)
            logging.info(f"Created output directory: {output_path}")
        query = (
            df.writeStream.format(
                "json"
            )  # Use "json" or "csv" depending on your requirement
            .outputMode("append")  # Options: append, complete, update
            .option("path", output_path)  # Path to save the output files
            .option(
                "checkpointLocation", f"{output_path}/checkpoint"
            )  # Directory for checkpointing
            .start()
        )
        logging.info(f"Streaming query started, writing to file at {output_path}.")
        query.awaitTermination()
    except Exception as e:
        logging.error(f"Failed to write streaming data to file: {e}")


# Main streaming logic
if __name__ == "__main__":
    spark_conn = create_spark_connection()

    if spark_conn:
        df = get_streaming_dataframe(spark_conn)

        if df:
            transformed_df = transform_streaming_data(df)

            # Print the streamed data to the console
            write_to_file(transformed_df, "/opt/spark/output")
