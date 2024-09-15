import dataclasses

from pyspark.pandas import read_parquet
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import to_date, col
from shared.utils import WeatherDataFields, TaxiSilverDataFields, Util, TaxiGoldDataFields
from shared.config import SILVER_TAXI_DATA_SCHEMA, WEATHER_DATA_SCHEMA

class TaxiDataStreamClient:

    def __init__(self, spark, input_path):
        self.spark = spark
        self.input_path = input_path

    def get_taxi_data_stream(self) -> DataFrame:
        return self.spark.readStream.format("parquet").option("path", self.input_path).schema(SILVER_TAXI_DATA_SCHEMA).load()



class WeatherDataStreamClient:
    def __init__(self, spark, input_path):
        self.spark = spark
        self.input_path = input_path

    def get_weather_data_stream(self) -> DataFrame:
        return self.spark.readStream.format("csv").option("header", True).option("delimiter", ",").option("path", self.input_path).schema(WEATHER_DATA_SCHEMA).load().withColumn("date", to_date(col(WeatherDataFields.datetime), "yyyy-MM-dd"))


class TaxiDataTransformer:

    @staticmethod
    def combine_weather_and_taxi_data(weather, taxi) -> DataFrame:
        # taxi = taxi.withWatermark(TaxiSilverDataFields.drop_off_datetime, "10 minutes")
        condition =to_date(TaxiSilverDataFields.drop_off_datetime) == weather["date"]
        return taxi.join(weather, on=condition)

    @staticmethod
    def store_agg_data(df: DataFrame, checkpoint_location, output_path) -> None:
        relevant_columns = [output_column.default for output_column in dataclasses.fields(TaxiGoldDataFields)]
        df = df.select(relevant_columns)

        query = df.writeStream.format("delta").outputMode("append").option(
            "checkpointLocation", checkpoint_location
        ).partitionBy(TaxiGoldDataFields.vendor_id).trigger(processingTime="100 seconds").start(output_path)
        query.awaitTermination()


def main() -> None:
    """Combines weather and taxi data."""
    from awsglue.job import Job

    job_args = Util.get_job_args(["INPUT_PATH", "INOUT_PATH_WEATHER", "OUTPUT_PATH", "CHECKPOINT_LOCATION", "JOB_NAME"])
    glue, spark = Util.get_spark_and_glue_session("taxi-gold-etl")
    job = Job(glue)
    job.init(job_args["JOB_NAME"], job_args)

    taxi_data = TaxiDataStreamClient(spark, job_args["INPUT_PATH"]).get_taxi_data_stream()
    weather_data = WeatherDataStreamClient(spark, job_args["INOUT_PATH_WEATHER"]).get_weather_data_stream()

    taxi_data_agg = TaxiDataTransformer.combine_weather_and_taxi_data(weather_data, taxi_data)
    TaxiDataTransformer.store_agg_data(taxi_data_agg, checkpoint_location=job_args["CHECKPOINT_LOCATION"], output_path=job_args["OUTPUT_PATH"])

    job.commit()



if __name__ == '__main__':
    main()