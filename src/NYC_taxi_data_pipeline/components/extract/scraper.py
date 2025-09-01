import requests
from bs4 import BeautifulSoup
import re
from datetime import datetime, timedelta
import io
import time
from typing import List
import pandas as pd
from pyspark.sql import SparkSession
import tempfile


class ExtractData:
    def __init__(self,start_date:str, end_date:str):

      self.start_date = datetime.strptime(start_date, "%Y-%m-%d")
      self.end_date = datetime.strptime(end_date, "%Y-%m-%d")
      self.spark = SparkSession.builder.appName('NYC_taxi').getOrCreate()


    def extract_nyc_yellow_taxi_data(self,
                 url:str ="https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page",):
      try:

        # Send GET request
        response = requests.get(url)
        soup = BeautifulSoup(response.content, "html.parser")

        # Regex pattern to match High Volume FHV files with date
        pattern = re.compile(r"(yellow_tripdata_)(\d{4}-\d{2})\.parquet", re.IGNORECASE)

        # Loop through all links
        download_links = []
        for link in soup.find_all("a", href=True):
            href = link["href"]
            match = pattern.search(href)
            if match:
                date_str = match.group(2)
                file_date = datetime.strptime(date_str, "%Y-%m")
                if file_date >= self.start_date and file_date <= self.end_date:
                    full_url = href if href.startswith("http") else f"https://www.nyc.gov{href}"
                    download_links.append((date_str, full_url))

        # Download and load each file into a DataFrame
        yellow_taxi_dfs = None

        for date_str, link in download_links:
            print(f"Downloading {date_str} from {link}")
            response = requests.get(link)
            if response.status_code == 200:
                # Saving file in a tmp filepath

                with tempfile.NamedTemporaryFile(delete=False, suffix=".parquet") as tmp_file:
                    tmp_file.write(response.content)
                    tmp_path = tmp_file.name

                df = self.spark.read.parquet(tmp_path)

                #adding to dataframe
                if yellow_taxi_dfs is None:
                    yellow_taxi_dfs = df
                else:
                  yellow_taxi_dfs=yellow_taxi_dfs.union(df)

            else:
                print(f"Failed to download {link}")

        if yellow_taxi_dfs:
          print(f"Data downloaded successfully: {yellow_taxi_dfs.count()} rows, {len(yellow_taxi_dfs.columns)} columns")
          return yellow_taxi_dfs
        
        else:
          print("No data found for the given date range.")
          return None

      except Exception as e:
        return (f"Error: {e}")
      

    def extract_nyc_network_data(self, API_KEY: str,
                                location: str = "New York NY United States",
                                base_url: str = "https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline"):

        start_date = self.start_date
        end_date = self.end_date

        current_date = start_date

        all_hourly_records = []
        metadata_captured = False
        full_json = {}

        while current_date <= end_date:
            next_month = (current_date.replace(day=28) + timedelta(days=4)).replace(day=1)
            month_end = min(next_month - timedelta(days=1), end_date)

            start_date_str = current_date.strftime("%Y-%m-%d")
            end_date_str = month_end.strftime("%Y-%m-%d")

            params = {
                "unitGroup": "us",
                "key": API_KEY,
                "include": "days,hours,current,alerts,stations",
                "contentType": "json"
            }

            url = f"{base_url}/{location}/{start_date_str}/{end_date_str}"
            print(f"Fetching data from {start_date_str} to {end_date_str}...")

            response = requests.get(url, params=params)

            if response.status_code == 200:
                data = response.json()

                # Capture Metadata once
                if not metadata_captured:
                  full_json.update({k: data.get(k) for k in [
                      "queryCost", "latitude", "longitude", "resolvedAddress",
                      "address", "timezone", "tzoffset", "description"
                  ]})
                  metadata_captured = True

                # Extract hourly data for this month only
                fields = ['datetime', 'temp', 'dew', 'humidity', 'precip', 'preciptype', 'snow', 'snowdepth', 'visibility']
                for day in data.get("days", []):
                    for hour in day.get("hours", []):
                        filtered_hour = {key: hour.get(key) for key in fields}
                        filtered_hour["day"] = day.get("datetime")
                        all_hourly_records.append(filtered_hour)

                print(f"Retrieved {len(data.get('days', []))} days.")

            elif response.status_code == 429:
                print(f" Rate limit hit ({response.status_code}): {response.text}")
            else:
                print(f"Error {response.status_code}: {response.text}")

            current_date = next_month
            time.sleep(3)

        if all_hourly_records:
          # Create Spark DataFrame from list of dicts
          df_hours = self.spark.createDataFrame(all_hourly_records)

          # Count rows and estimate days (assuming 24 hourly records per day)
          total_rows = df_hours.count()
          print(f"\nRetrieved data contains {total_rows // 24} days of data.")

          return df_hours
        else:
          print("No data retrieved.")
