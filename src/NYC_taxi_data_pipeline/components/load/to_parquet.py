import os
from pyspark.sql import DataFrame, SparkSession

class LoadData:
    def __init__(self, transformed_data: DataFrame, file_name: str, spark: SparkSession):
        self.transformed_data = transformed_data
        self.file_name = file_name
        self.spark = spark

    def save(self):
        try:
            if not os.path.exists(self.file_name) or len(os.listdir(self.file_name)) == 0:
                # Save as new Parquet dataset
                self.transformed_data.write.mode("overwrite").parquet(self.file_name)
                print("✅ Data saved successfully (new file or empty file)")
            else:
                # Read existing Parquet dataset
                existing_data = self.spark.read.parquet(self.file_name)

                # Union and drop duplicates
                combined_data = existing_data.unionByName(self.transformed_data).dropDuplicates()

                # Overwrite with combined data
                combined_data.write.mode("overwrite").parquet(self.file_name)
                print("✅ Data appended successfully")

            return (self.transformed_data.count(), len(self.transformed_data.columns))

        except Exception as e:
            raise e
