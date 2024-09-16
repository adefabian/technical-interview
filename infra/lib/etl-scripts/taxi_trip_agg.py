"""Agg taxi and weather data."""
import dataclasses

from pyspark.sql import DataFrame
from pyspark.sql.functions import col
from pyspark.sql.functions import to_date
from shared.config import SILVER_TAXI_DATA_SCHEMA
from shared.config import WEATHER_DATA_SCHEMA
from shared.utils import TaxiGoldDataFields
from shared.utils import TaxiSilverDataFields
from shared.utils import Util
from shared.utils import WeatherDataFields


class TaxiDataStreamClient:
    """Taxi silver data stream client."""

    def __init__(self, spark, input_path):
        """Init method."""
        self.spark = spark
        self.input_path = input_path

    def get_taxi_data_stream(self) -> DataFrame:
        """Get the taxi silver data stream."""
        return (
            self.spark.readStream.format("parquet")
            .option("path", self.input_path)
            .schema(SILVER_TAXI_DATA_SCHEMA)
            .load()
        )


class WeatherDataStreamClient:
    """Weather data stream client."""

    def __init__(self, spark, input_path):
        """Init method."""
        self.spark = spark
        self.input_path = input_path

    def get_weather_data_stream(self) -> DataFrame:
        """Get weather data stream."""
        return (
            self.spark.readStream.format("csv")
            .option("header", True)
            .option("delimiter", ",")
            .option("path", self.input_path)
            .schema(WEATHER_DATA_SCHEMA)
            .load()
            .withColumn("date", to_date(col(WeatherDataFields.datetime), "yyyy-MM-dd"))
        )


class TaxiDataTransformer:
    """Transform weather and taxi data."""

    @staticmethod
    def combine_weather_and_taxi_data(weather, taxi) -> DataFrame:
        """Combine the two dataframes."""
        # taxi = taxi.withWatermark(TaxiSilverDataFields.drop_off_datetime, "10 minutes")
        condition = to_date(TaxiSilverDataFields.drop_off_datetime) == weather["date"]
        return taxi.join(weather, on=condition)

    @staticmethod
    def store_agg_data(df: DataFrame, checkpoint_location, output_path) -> None:
        """Store the combined dataframe to s3."""
        relevant_columns = [output_column.default for output_column in dataclasses.fields(TaxiGoldDataFields)]
        df = df.select(relevant_columns)

        query = (
            df.writeStream.format("delta")
            .outputMode("append")
            .option("checkpointLocation", checkpoint_location)
            .partitionBy(TaxiGoldDataFields.vendor_id)
            .trigger(processingTime="100 seconds")
            .start(output_path)
        )
        query.awaitTermination()


def main() -> None:
    """Combine weather and taxi data."""
    from awsglue.job import Job

    job_args = Util.get_job_args(["INPUT_PATH", "INOUT_PATH_WEATHER", "OUTPUT_PATH", "CHECKPOINT_LOCATION", "JOB_NAME"])
    glue, spark = Util.get_spark_and_glue_session("taxi-gold-etl")
    job = Job(glue)
    job.init(job_args["JOB_NAME"], job_args)

    taxi_data = TaxiDataStreamClient(spark, job_args["INPUT_PATH"]).get_taxi_data_stream()
    weather_data = WeatherDataStreamClient(spark, job_args["INOUT_PATH_WEATHER"]).get_weather_data_stream()

    taxi_data_agg = TaxiDataTransformer.combine_weather_and_taxi_data(weather_data, taxi_data)
    TaxiDataTransformer.store_agg_data(
        taxi_data_agg, checkpoint_location=job_args["CHECKPOINT_LOCATION"], output_path=job_args["OUTPUT_PATH"]
    )

    job.commit()


if __name__ == "__main__":
    main()
