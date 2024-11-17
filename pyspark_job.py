import os
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from dotenv import load_dotenv

load_dotenv()

# Set up logging
logging.basicConfig(level=logging.INFO)

sfOptions = {
    "sfURL": f"https://{os.getenv('SNOWFLAKE_ACCOUNT')}.snowflakecomputing.com",
    "sfUser": os.getenv("SNOWFLAKE_USER"),
    "sfPassword": os.getenv("SNOWFLAKE_PASSWORD"),
    "sfDatabase": os.getenv("SNOWFLAKE_DATABASE"),
    "sfSchema": os.getenv("SNOWFLAKE_SCHEMA"),
    "sfWarehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
    "sfRole": os.getenv("SNOWFLAKE_ROLE"),
}


def insert_data(session, **kwargs):
    logging.info("Inserting data...")
    try:
        session.sql(
            f"INSERT INTO {os.getenv('SNOWFLAKE_DATABASE')}.{os.getenv('SNOWFLAKE_SCHEMA')}.{os.getenv('SNOWFLAKE_TABLE')} "
            f"(appointment_id, patient_id, patient_name, gender, age, address, symptoms, diagnosis, treatment_plan, appointment_date, doctor_name, hospital, insurance_provider, payment_method) "
            f"VALUES ('{kwargs['appointment_id']}', '{kwargs['patient_id']}', '{kwargs['patient_name']}', '{kwargs['gender']}', {kwargs['age']}, '{kwargs['address']}', '{kwargs['symptoms']}', '{kwargs['diagnosis']}', '{kwargs['treatment_plan']}', '{kwargs['appointment_date']}', '{kwargs['doctor_name']}', '{kwargs['hospital']}', '{kwargs['insurance_provider']}', '{kwargs['payment_method']}')"
        )
        logging.info(
            f"Data inserted successfully for appointment_id: {kwargs['appointment_id']}"
        )
    except Exception as e:
        logging.error(f"Failed to insert data: {e}")


def create_spark_connection():
    try:
        spark_conn = SparkSession.builder.appName("pyspark-job").getOrCreate()
        logging.info("Spark session created successfully")
        return spark_conn
    except Exception as e:
        logging.error(f"Failed to create Spark session: {e}")
        return None


def connect_to_kafka(spark_conn):
    try:
        spark_df = (
            spark_conn.readStream.format("kafka")
            .option("kafka.bootstrap.servers", "localhost:9094")
            .option("subscribe", "cdc.public.appointments")
            .option("startingOffsets", "earliest")
            .load()
        )
        spark_df.printSchema()
        logging.info("Kafka dataframe created successfully")
        return spark_df
    except Exception as e:
        logging.error(f"Failed to create Kafka dataframe: {e}")
        return None


def create_snowflake_connection(spark_conn):
    session = spark_conn.read.format("snowflake").options(**sfOptions).load()
    return session


def create_selection_df_from_kafka(spark_df):
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
        spark_df.selectExpr("CAST(value AS STRING)")
        .select(from_json(col("value"), schema).alias("data"))
        .select("data.*")
    )
    logging.info(f"Selection dataframe created: {sel}")

    return sel


if __name__ == "__main__":
    spark_conn = create_spark_connection()

    if spark_conn is not None:
        spark_df = connect_to_kafka(spark_conn)

        if spark_df is not None:
            selection_df = create_selection_df_from_kafka(spark_df)

            # Create Snowflake connection options
            session = create_snowflake_connection(spark_conn)

            if session is not None:
                logging.info("Streaming is being started...")

                # Configure streaming to write to Snowflake
                streaming_query = (
                    selection_df.writeStream.format("snowflake")
                    .option("checkpointLocation", "tmp/checkpoint")
                    .option("dbtable", os.getenv("SNOWFLAKE_TABLE"))
                    .options(**sfOptions)
                    .start()
                )

                streaming_query.awaitTermination()
            else:
                logging.error("Failed to create Snowflake session.")
        else:
            logging.error("Failed to connect to Kafka.")
    else:
        logging.error("Failed to create Spark connection.")
