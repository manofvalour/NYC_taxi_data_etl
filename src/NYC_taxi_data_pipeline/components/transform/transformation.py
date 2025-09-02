

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (col, to_timestamp, concat_ws, 
                                   when, round as spark_round, 
                                   lit, create_map)
from pyspark.sql.types import StringType
import logging
from itertools import chain
import requests
import tempfile
import os

class DataTransformation:
    def __init__(self, nyc_taxi_data: DataFrame, nyc_weather_data: DataFrame, spark: SparkSession):
        self.nyc_taxi_data = nyc_taxi_data
        self.nyc_weather_data = nyc_weather_data
        self.spark = spark

    def transform_nyc_yellow_taxi_data(self) -> DataFrame:
        df = self.nyc_taxi_data
        try:
          # VendorID mapping
          vendor_id = {
              1: "Creative Mobile Technologies, LLC",
              2: "Curb Mobility, LLC",
              6: "Myle Technologies Inc",
              7: "Helix"
          }
          df= df.withColumn("VendorID", col("VendorID").cast(StringType()))
          ## mappping vendor_id to the VendorID column in df
          df= df.withColumn("VendorID",when(col("VendorID") == 1, "Creative Mobile Technologies, LLC")
          .when(col("VendorID") == 2, "Curb Mobility, LLC").when(col("VendorID") == 6, "Myle Technologies Inc").when(col('VendorID') == 7, "Helix")
          .otherwise("Unknown"))
          #df = df.replace(vendor_id, subset=["VendorID"])

        except Exception as e:
          logging.error(f"Error in VendorID mapping: {e}")
          raise e

        # RatecodeID mapping
        ratecode = {
            1: "Standard rate",
            2: "JFK",
            3: "Newark",
            4: "Nassau or Westchester",
            5: "Negotiated fare",
            6: "Group ride",
            99: "Null/unknown"
        }
        df = df.withColumn("RatecodeID", col("RatecodeID").cast(StringType()))

        mapping_expr = create_map([lit(x) for x in chain(*ratecode.items())])
        df = df.withColumn("RatecodeID", mapping_expr[col("RatecodeID")])


        # Store and forward flag mapping
        store_and_forward = {
            "N": "No",
            "Y": "Yes"
        }

        df = df.withColumn("store_and_fwd_flag", col("store_and_fwd_flag").cast(StringType()))
        mapping_expr= create_map([lit(x) for x in chain(*store_and_forward.items())])
        df = df.withColumn("store_and_fwd_flag", mapping_expr[col("store_and_fwd_flag")])

        #df = df.replace(store_and_forward, subset=["store_and_fwd_flag"])

        # Payment type mapping
        payment_type = {
            0: "Flex Fair Trip",
            1: "Credit card",
            2: "Cash",
            3: "No charge",
            4: "Dispute",
            5: "Unknown",
            6: "Voided trip"
        }
        df= df.withColumn("payment_type", col("payment_type").cast(StringType()))
        mapping_expr = create_map([lit(x) for x in chain(*payment_type.items())])
        df = df.withColumn("payment_type", mapping_expr[col("payment_type")])
        #df = df.replace(payment_type, subset=["payment_type"])

        # Convert pickup and dropoff datetime
        df = df.withColumn("pickup_datetime", to_timestamp(col("tpep_pickup_datetime"))) \
               .withColumn("dropoff_datetime", to_timestamp(col("tpep_dropoff_datetime")))

        # Load lookup table into Spark
        lookup_url = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"
        try:
            response = requests.get(lookup_url)
            response.raise_for_status() # Raise an exception for bad status codes

            with tempfile.NamedTemporaryFile(delete=False, suffix=".csv") as tmp_file:
                tmp_file.write(response.content)
                tmp_path = tmp_file.name

            lookup_table = self.spark.read.csv(
                tmp_path,
                header=True,
                inferSchema=True
            )
            os.remove(tmp_path) # Clean up the temporary file

        except requests.exceptions.RequestException as e:
            logging.error(f"Error downloading lookup table: {e}")
            raise e
        except Exception as e:
            logging.error(f"Error reading lookup table: {e}")
            raise e


        # Join for pickup location
        df = df.join(
            lookup_table.withColumnRenamed("LocationID", "PULocationID_lookup")
                        .withColumnRenamed("Borough", "PULocation_borough")
                        .withColumnRenamed("Zone", "PULocation_zone")
                        .withColumnRenamed("service_zone", "PULocation_service_zone"),
            df.PULocationID == col("PULocationID_lookup"),
            "left"
        )

        # Join for dropoff location
        df = df.join(
            lookup_table.withColumnRenamed("LocationID", "DOLocationID_lookup")
                        .withColumnRenamed("Borough", "DOLocation_borough")
                        .withColumnRenamed("Zone", "DOLocation_zone")
                        .withColumnRenamed("service_zone", "DOLocation_service_zone"),
            df.DOLocationID == col("DOLocationID_lookup"),
            "left"
        )

        # Drop unnecessary columns
        df = df.drop("PULocationID", "DOLocationID",
                     "tpep_pickup_datetime", "tpep_dropoff_datetime",
                     "PULocationID_lookup", "DOLocationID_lookup")

        return df

    def transform_weather_data(self) -> DataFrame:
        df = self.nyc_weather_data

        # Combine day + datetime into one timestamp
        df = df.withColumn(
            "datetime",
            to_timestamp(concat_ws(" ", col("day"), col("datetime")))
        ).drop("day")

        return df

    def merge_data(self) -> DataFrame:
        taxi_df = self.transform_nyc_yellow_taxi_data()
        weather_df = self.transform_weather_data()

        # Round pickup time to nearest hour
        taxi_df = taxi_df.withColumn("new_pickup_datetime", spark_round(col("pickup_datetime").cast("double") / 3600) * 3600)
        taxi_df = taxi_df.withColumn("new_pickup_datetime", to_timestamp(col("new_pickup_datetime")))

        # Join on datetime
        merged_df = taxi_df.join(
            weather_df,
            taxi_df.new_pickup_datetime == weather_df.datetime,
            "left"
        ).drop("new_pickup_datetime", "datetime")

        return merged_df

    def transform(self) -> DataFrame:
        return self.merge_data()