import pandas as pd


class TaxiDataCleaner:
    """Helper class for preparing the taxi data set."""

    @staticmethod
    def transform_taxi_data(taxi_data: pd.DataFrame, taxi_zone_map: pd.DataFrame) -> pd.DataFrame:
        return TaxiDataCleaner._combine_taxi_data_with_taxi_zones(taxi_data, taxi_zone_map)

    @staticmethod
    def _combine_taxi_data_with_taxi_zones(taxi_data: pd.DataFrame, taxi_zone_map: pd.DataFrame) -> pd.DataFrame:
        taxi_zone_map = taxi_zone_map.set_index("LocationID")["Zone"].to_dict()

        taxi_data["pickup_location"] = taxi_data["PULocationID"].map(taxi_zone_map)
        taxi_data["drop_off_location"] = taxi_data["DOLocationID"].map(taxi_zone_map)
        return taxi_data



def main() -> None:
    taxi_data = pd.read_parquet("./resources/yellow_taxi_trip_data/yellow_tripdata_2024-01.parquet")
    taxi_zone_data = pd.read_csv("./resources/yellow_taxi_trip_data/taxi_zone_lookup_table.csv")

    merged_taxi_data = TaxiDataCleaner.transform_taxi_data(taxi_data, taxi_zone_data)
    merged_taxi_data.to_parquet("./transformed_nyc_taxi_data.parquet")


if __name__ == "__main__":
    main()