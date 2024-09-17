"""Ingest the raw taxi trip data into delta lake."""
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from shared.config import RAW_TAXI_DATA_SCHEMA
from shared.utils import TaxiRawDataFields
from shared.utils import Util


class TaxiTripIngestionClient:
    """Raw taxi data ingest client."""

    def __init__(self, glue, spark: SparkSession, path: str, checkpoint_location: str, output_path: str):
        """Create taxi ingestion client obj."""
        self.glue = glue
        self.spark = spark
        self.input_path = path
        self.checkpoint_location = checkpoint_location
        self.output_path = output_path

    def get_taxi_data_stream(self) -> DataFrame:
        """Create a spark stream by reading all data in s3 path."""
        return (
            self.spark.readStream.format("parquet").option("path", self.input_path).schema(RAW_TAXI_DATA_SCHEMA).load()
        )

    def write_stream_to_s3(self, df: DataFrame) -> None:
        """Write the batch to s3."""
        (
            df.writeStream.format("delta")
            .option("checkpointLocation", "/path/to/bronze/checkpoint")
            .outputMode("append")
            .option("mergeSchema", "true")
            .start(self.output_path)
            .awaitTermination()
        )


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
        "output_path": job_args["OUTPUT_PATH"],
    }

    taxi_data_stream_client = TaxiTripIngestionClient(**taxi_trip_client_args)
    taxi_data_stream = taxi_data_stream_client.get_taxi_data_stream()
    taxi_data_stream_client.write_stream_to_s3(taxi_data_stream)

    job.commit()


if __name__ == "__main__":
    main()
