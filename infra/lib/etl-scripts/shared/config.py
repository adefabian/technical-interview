"""Config class."""
from pyspark.sql.types import DoubleType
from pyspark.sql.types import IntegerType
from pyspark.sql.types import LongType
from pyspark.sql.types import StringType
from pyspark.sql.types import StructField
from pyspark.sql.types import StructType
from pyspark.sql.types import TimestampType

from .utils import TaxiRawDataFields, TaxiSilverDataFields

RAW_TAXI_DATA_SCHEMA = StructType(
    [
        StructField(TaxiRawDataFields.vendor_id, IntegerType(), True),
        StructField(TaxiRawDataFields.pickup_datetime, TimestampType(), True),
        StructField(TaxiRawDataFields.drop_off_datetime, TimestampType(), True),
        StructField(TaxiRawDataFields.passenger_count, DoubleType(), True),
        StructField(TaxiRawDataFields.trip_distance, DoubleType(), True),
        StructField(TaxiRawDataFields.rate_code_id, DoubleType(), True),
        StructField(TaxiRawDataFields.store_and_fwd_flag, StringType(), True),
        StructField(TaxiRawDataFields.pu_location_id, IntegerType(), True),
        StructField(TaxiRawDataFields.do_location_id, IntegerType(), True),
        StructField(TaxiRawDataFields.payment_type, LongType(), True),
        StructField(TaxiRawDataFields.fare_amount, DoubleType(), True),
        StructField(TaxiRawDataFields.extra, DoubleType(), True),
        StructField(TaxiRawDataFields.mta_tax, DoubleType(), True),
        StructField(TaxiRawDataFields.tip_amount, DoubleType(), True),
        StructField(TaxiRawDataFields.tolls_amount, DoubleType(), True),
        StructField(TaxiRawDataFields.improvement_surcharge, DoubleType(), True),
        StructField(TaxiRawDataFields.total_amount, DoubleType(), True),
        StructField(TaxiRawDataFields.congestion_surcharge, DoubleType(), True),
        StructField(TaxiRawDataFields.airport_fee, DoubleType(), True),
        StructField(TaxiRawDataFields.pickup_location, StringType(), True),
        StructField(TaxiRawDataFields.drop_off_location, StringType(), True),
    ]
)


SILVER_TAXI_DATA_SCHEMA = StructType(
    [
        StructField(TaxiSilverDataFields.vendor_id, StringType(), True),
        StructField(TaxiSilverDataFields.pickup_datetime, TimestampType(), True),
        StructField(TaxiSilverDataFields.drop_off_datetime, TimestampType(), True),
        StructField(TaxiSilverDataFields.passenger_count, DoubleType(), True),
        StructField(TaxiSilverDataFields.payment_type, LongType(), True),
        StructField(TaxiSilverDataFields.fare_amount, DoubleType(), True),
        StructField(TaxiSilverDataFields.pickup_location, StringType(), True),
        StructField(TaxiSilverDataFields.drop_off_location, StringType(), True),
    ]
)

WEATHER_DATA_SCHEMA = StructType([
    StructField("name", StringType(), True),
    StructField("datetime", StringType(), True),
    StructField("tempmax", StringType(), True),
    StructField("tempmin", StringType(), True),
    StructField("temp", StringType(), True),
    StructField("feelslikemax", StringType(), True),
    StructField("feelslikemin", StringType(), True),
    StructField("feelslike", StringType(), True),
    StructField("dew", StringType(), True),
    StructField("humidity", StringType(), True),
    StructField("precip", StringType(), True),
    StructField("precipprob", StringType(), True),
    StructField("precipcover", StringType(), True),
    StructField("preciptype", StringType(), True),
    StructField("snow", StringType(), True),
    StructField("snowdepth", StringType(), True),
    StructField("windgust", StringType(), True),
    StructField("windspeed", StringType(), True),
    StructField("winddir", StringType(), True),
    StructField("sealevelpressure", StringType(), True),
    StructField("cloudcover", StringType(), True),
    StructField("visibility", StringType(), True),
    StructField("solarradiation", StringType(), True),
    StructField("solarenergy", StringType(), True),
    StructField("uvindex", StringType(), True),
    StructField("severerisk", StringType(), True),
    StructField("sunrise", StringType(), True),
    StructField("sunset", StringType(), True),
    StructField("moonphase", StringType(), True),
    StructField("conditions", StringType(), True),
    StructField("description", StringType(), True),
    StructField("icon", StringType(), True),
    StructField("stations", StringType(), True),
])