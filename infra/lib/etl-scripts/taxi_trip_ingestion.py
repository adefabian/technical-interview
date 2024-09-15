"""Ingest the raw taxi trip data into delta lake."""
from pandas.io.formats.info import DataFrameInfo
from pyspark.sql import DataFrame
from pyspark.sql.types import DoubleType
from pyspark.sql.types import IntegerType
from pyspark.sql.types import LongType
from pyspark.sql.types import StringType
from pyspark.sql.types import StructField
from pyspark.sql.types import StructType
from pyspark.sql.types import TimestampType
from shared.utils import Util


class TaxiTripIngestionClient:
    """Raw taxi data ingest client."""

    def __init__(self, glue, spark, path, checkpoint_location, db, table):
        """Create taxi ingestion client obj,."""
        self.glue = glue
        self.spark = spark
        self.input_path = path
        self.checkpoint_location = checkpoint_location
        self.db = db
        self.table = table

    def get_taxi_data_stream(self) -> DataFrameInfo:
        """Create a spark stream by reading all data in s3 path."""
        return (
            self.spark.readStream.format("parquet")
            .option("path", self.input_path)
            .schema(self._get_taxi_data_schema())
            .load()
        )

    def write_stream_to_s3(self, df: DataFrame) -> None:
        """Write the batch to s3."""
        self.glue.forEachBatch(
            frame=df,
            batch_function=self._process_batch,
            options={"windowSize": "100 seconds", "checkpointLocation": self.checkpoint_location},
        )

    def _get_taxi_data_schema(self) -> StructType:
        return StructType(
            [
                StructField("VendorID", IntegerType(), True),
                StructField("tpep_pickup_datetime", TimestampType(), True),
                StructField("tpep_dropoff_datetime", TimestampType(), True),
                StructField("passenger_count", DoubleType(), True),
                StructField("trip_distance", DoubleType(), True),
                StructField("RatecodeID", DoubleType(), True),
                StructField("store_and_fwd_flag", StringType(), True),
                StructField("PULocationID", IntegerType(), True),
                StructField("DOLocationID", IntegerType(), True),
                StructField("payment_type", LongType(), True),
                StructField("fare_amount", DoubleType(), True),
                StructField("extra", DoubleType(), True),
                StructField("mta_tax", DoubleType(), True),
                StructField("tip_amount", DoubleType(), True),
                StructField("tolls_amount", DoubleType(), True),
                StructField("improvement_surcharge", DoubleType(), True),
                StructField("total_amount", DoubleType(), True),
                StructField("congestion_surcharge", DoubleType(), True),
                StructField("Airport_fee", DoubleType(), True),
                StructField("pickup_location", StringType(), True),
                StructField("drop_off_location", StringType(), True),
            ]
        )

    def _process_batch(self, df, batchId):
        from awsglue import DynamicFrame

        if df.count() > 0:
            dy = DynamicFrame.fromDF(df, self.glue, "from_data_frame")
            additionalOptions_datasink = {"enableUpdateCatalog": True, "partitionKeys": ["VendorID"]}

            datasink = self.glue.write_dynamic_frame.from_catalog(
                frame=dy,
                database=self.db,
                table_name=self.table,
                transformation_ctx="datasink_kafka",
                additional_options=additionalOptions_datasink,
            )

            self.spark.sql(f"MSCK REPAIR TABLE {self.db}.{self.table}")


def main() -> None:
    """Process all new taxi data and stores it inside delta lake."""
    from awsglue.job import Job

    job_args = Util.get_job_args(
        ["INPUT_PATH", "OUTPUT_PATH", "CHECKPOINT_LOCATION", "TAXI_DB", "BRONZE_TAXI_TABLE", "JOB_NAME"]
    )
    glue, spark = Util.get_spark_and_glue_session("taxi-bronze-etl")
    job = Job(glue)
    job.init(job_args["JOB_NAME"], job_args)

    taxi_trip_client_args = {
        "glue": glue,
        "spark": spark,
        "path": job_args["INPUT_PATH"],
        "checkpoint_location": job_args["CHECKPOINT_LOCATION"],
        "db": job_args["TAXI_DB"],
        "table": job_args["BRONZE_TAXI_TABLE"],
    }

    taxi_data_stream_client = TaxiTripIngestionClient(**taxi_trip_client_args)
    taxi_data_stram = taxi_data_stream_client.get_taxi_data_stream()
    taxi_data_stream_client.write_stream_to_s3(taxi_data_stram)

    job.commit()


if __name__ == "__main__":
    main()
