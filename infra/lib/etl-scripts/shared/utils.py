"""Utils class."""
import sys
from dataclasses import dataclass

from pyspark.sql import SparkSession


class Util:
    """Utils class."""

    @staticmethod
    def get_spark_and_glue_session(app_name: str):
        """Generate spark and glue session."""
        from awsglue.context import GlueContext

        spark = SparkSession.builder.appName(app_name).getOrCreate()
        glue = GlueContext(spark)
        return glue, spark

    @staticmethod
    def get_job_args(job_args: [str]) -> dict[str, str]:
        """
        Get the job arguments from the glue job.

        :param job_args: job args
        :return job arguments passed to the glue job
        """
        from awsglue.utils import getResolvedOptions

        return getResolvedOptions(
            sys.argv,
            job_args,
        )


@dataclass(frozen=True)
class TaxiRawDataFields:
    """Helper class for storing the raw column names."""

    vendor_id: str = "VendorID"
    pickup_datetime: str = "tpep_pickup_datetime"
    drop_off_datetime: str = "tpep_dropoff_datetime"
    passenger_count: str = "passenger_count"
    trip_distance: str = "trip_distance"
    rate_code_id: str = "RatecodeID"
    store_and_fwd_flag: str = "store_and_fwd_flag"
    pu_location_id: str = "PULocationID"
    do_location_id: str = "DOLocationID"
    payment_type: str = "payment_type"
    fare_amount: str = "fare_amount"
    extra: str = "extra"
    mta_tax: str = "mta_tax"
    tip_amount: str = "tip_amount"
    tolls_amount: str = "tolls_amount"
    improvement_surcharge: str = "improvement_surcharge"
    total_amount: str = "total_amount"
    congestion_surcharge: str = "congestion_surcharge"
    airport_fee: str = "Airport_fee"
    pickup_location: str = "pickup_location"
    drop_off_location: str = "drop_off_location"


@dataclass(frozen=True)
class TaxiSilverDataFields:
    """Helper class for storing the raw column names."""

    vendor_id: str = "vendor_id"
    pickup_datetime: str = "pickup_timestamp"
    drop_off_datetime: str = "drop_off_timestamp"
    passenger_count: str = "passenger_count"
    payment_type: str = "payment_type"
    fare_amount: str = "fare_amount"
    pickup_location: str = "pickup_location"
    drop_off_location: str = "drop_off_location"


@dataclass(frozen=True)
class TaxiGoldDataFields(TaxiSilverDataFields):
    temp_max: str = "tempmax"
    temp_min: str = "tempmin"

@dataclass(frozen=True)
class WeatherDataFields:
    datetime: str = "datetime"
