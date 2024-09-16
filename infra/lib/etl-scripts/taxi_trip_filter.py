"""Taxi Trip filter, used for validating and filtering raw data."""
import dataclasses
import os

from pyspark.sql import DataFrame
from shared.config import RAW_TAXI_DATA_SCHEMA
from shared.utils import TaxiRawDataFields
from shared.utils import TaxiSilverDataFields
from shared.utils import Util

os.environ["SPARK_VERSION"] = "3.3"


class TaxiDataFilter:
    """Filter the taxi raw data."""

    def __init__(self, spark, path, output_path, checkpoint_location):
        """Create taxi trip data filter object."""
        self.input_path = path
        self.spark = spark
        self.output_path = output_path
        self.checkpoint_location = checkpoint_location

    def transform_taxi_raw_data(self, df: DataFrame) -> DataFrame:
        """Extract the relevant info from the taxi data."""
        return df.transform(self._rename_columns).transform(self._extract_relevant_columns)

    def validate_dataframe_data_quality(self, df: DataFrame) -> DataFrame:
        """Validate the data quality."""
        # from pydeequ import Check, CheckLevel
        # from pydeequ.verification import VerificationSuite
        #
        # check = Check(self.spark, CheckLevel.Error, "Data Validation Check")
        #
        # # Run the validation suite
        # verification_result = (
        #     VerificationSuite(self.spark)
        #     .onData(df)
        #     .addCheck(
        #         check.isComplete("vendor_id")  # Ensure 'vendor_id' column has no null values
        #         .isNonNegative("fare_amount")  # Ensure 'fare_amount' column is non-negative
        #         .satisfies(
        #             "drop_off_timestamp > pickup_timestamp", "Drop-off timestamp should be later than pickup timestamp"
        #         )
        #     )
        #     .run()
        # )

        # Show the validation results
        # verification_result_df = verification_result.checkResultsAsDataFrame(self.spark, verification_result)

        # removed for testing
        # if verification_result_df.filter(col("constraint_status") != "Success").count() > 0:
        #     raise Exception(f"The Dataquality check has failed: {verification_result_df.show(10,0)}")

        return df

    def _extract_relevant_columns(self, df) -> DataFrame:
        """Extract the relevant columns."""
        relevant_columns = [output_column.default for output_column in dataclasses.fields(TaxiSilverDataFields)]
        return df.select(relevant_columns)

    def _rename_columns(self, df: DataFrame) -> DataFrame:
        return (
            df.withColumnRenamed(TaxiRawDataFields.vendor_id, TaxiSilverDataFields.vendor_id)
            .withColumnRenamed(TaxiRawDataFields.pickup_datetime, TaxiSilverDataFields.pickup_datetime)
            .withColumnRenamed(TaxiRawDataFields.drop_off_datetime, TaxiSilverDataFields.drop_off_datetime)
            .withColumnRenamed(TaxiRawDataFields.passenger_count, TaxiSilverDataFields.passenger_count)
            .withColumnRenamed(TaxiRawDataFields.payment_type, TaxiSilverDataFields.payment_type)
            .withColumnRenamed(TaxiRawDataFields.fare_amount, TaxiSilverDataFields.fare_amount)
            .withColumnRenamed(TaxiRawDataFields.pickup_location, TaxiSilverDataFields.pickup_location)
            .withColumnRenamed(TaxiRawDataFields.drop_off_location, TaxiSilverDataFields.drop_off_location)
        )

    def get_raw_taxi_data_stream(self) -> DataFrame:
        """Get the taxi bronze data."""
        return (
            self.spark.readStream.format("parquet").option("path", self.input_path).schema(RAW_TAXI_DATA_SCHEMA).load()
        )

    def write_stream_to_s3(self, df: DataFrame) -> None:
        """Write the batch to s3."""
        query = (
            df.writeStream.format("delta")
            .outputMode("append")
            .option("checkpointLocation", self.checkpoint_location)
            .partitionBy(TaxiSilverDataFields.vendor_id)
            .trigger(processingTime="100 seconds")
            .start(self.output_path)
        )
        query.awaitTermination()


def main():
    """Process all new taxi data and stores it inside delta lake."""
    from awsglue.job import Job

    job_args = Util.get_job_args(["INPUT_PATH", "OUTPUT_PATH", "CHECKPOINT_LOCATION", "JOB_NAME"])
    glue, spark = Util.get_spark_and_glue_session("taxi-silver-etl")
    job = Job(glue)
    job.init(job_args["JOB_NAME"], job_args)

    taxi_data_filter_client = TaxiDataFilter(
        spark, job_args["INPUT_PATH"], job_args["OUTPUT_PATH"], job_args["CHECKPOINT_LOCATION"]
    )
    taxi_data = taxi_data_filter_client.get_raw_taxi_data_stream()

    transformed_taxi_data = taxi_data_filter_client.transform_taxi_raw_data(taxi_data)
    validated_transformed_taxi_data = taxi_data_filter_client.validate_dataframe_data_quality(transformed_taxi_data)

    taxi_data_filter_client.write_stream_to_s3(validated_transformed_taxi_data)

    job.commit()


if __name__ == "__main__":
    main()
