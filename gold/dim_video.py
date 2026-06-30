import logging
import os
import sys
from dotenv import load_dotenv

os.environ["PYSPARK_PYTHON"] = r"C:\Users\harsh\AppData\Local\Programs\Python\Python310\python.exe"
os.environ["PYSPARK_DRIVER_PYTHON"] = r"C:\Users\harsh\AppData\Local\Programs\Python\Python310\python.exe"

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.functions import col,year,month, quarter,dayofmonth, dayofweek, weekofyear, date_format,when
from bronze.batch_ingest import create_spark_session

load_dotenv("config/secrets_template.env")
bronze_path = os.getenv("BRONZE_PATH")
silver_path = os.getenv("SILVER_PATH")
gold_path = os.getenv("GOLD_PATH")

class DimVideo:

    def __init__(self,spark,silver_path,gold_path):
        self.spark = spark
        self.silver_path = silver_path
        self.gold_path = gold_path

    def build(self,silver_path, gold_path):

        df_video = self.spark.read.format("delta").load(f"{self.silver_path}/videos")

        from pyspark.sql.functions import monotonically_increasing_id

        df_video = df_video.withColumn("video_sk",monotonically_increasing_id())

        df_video.write.format("delta").mode("overwrite").save(f"{gold_path}/dim_video")

        return df_video
    

if __name__=="__main__":

    spark = create_spark_session()

    video = DimVideo(spark=spark, silver_path = silver_path, gold_path=gold_path)

    video.build(silver_path,gold_path)




        