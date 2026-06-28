import logging
import os
import sys
from dotenv import load_dotenv

os.environ["PYSPARK_PYTHON"] = r"C:\Users\harsh\AppData\Local\Programs\Python\Python310\python.exe"
os.environ["PYSPARK_DRIVER_PYTHON"] = r"C:\Users\harsh\AppData\Local\Programs\Python\Python310\python.exe"

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from bronze.batch_ingest import create_spark_session

load_dotenv("config/secrets_template.env")
bronze_path = os.getenv("BRONZE_PATH")
silver_path = os.getenv("SILVER_PATH")


class Transform_video:

    def __init__(self, spark : SparkSession, bronze_path : str, silver_path : str):

        self.spark = spark
        self.bronze_path = bronze_path
        self.silver_path = silver_path

    def read_bronze(self):

        path =  f"{self.bronze_path}/videos"

        df = self.spark.read.format("delta").load(path)

        return df
    
    def clean(self, df : DataFrame):

        df = df.dropna(subset = ["video_id"])

        return df
    
    
    def enrich(self,df):

        df = df.withColumn("content_type", F.when(F.year(F.col("published_date"))<2025,"resurrected")\
                           .otherwise("new")
                           )
        
        return df

    def write_silver(self,df):

        df.write.format("delta").mode("overwrite").\
            partitionBy("published_date").save(f"{self.silver_path}/videos")

    def run(self):

        df = self.read_bronze()
        df = self.clean(df)
        df = self.enrich(df)
        self.write_silver(df)

if __name__== "__main__":

    spark = create_spark_session()
    video = Transform_video(
    spark=spark,
    bronze_path=bronze_path,
    silver_path=silver_path)

    video.run()
        