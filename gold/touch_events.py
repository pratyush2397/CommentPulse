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
path = load_dotenv("config/events.csv")
bronze_path = os.getenv("BRONZE_PATH")
silver_path = os.getenv("SILVER_PATH")
gold_path = os.getenv("GOLD_PATH")

def build_dim_events(spark,gold_path):

    csv_path = "config/events.csv"
    df_event = spark.read.format("csv").option("header",True).option("inferSchema",True).load(csv_path)

    from pyspark.sql.functions import monotonically_increasing_id

    df_event = df_event.withColumn("date_sk",monotonically_increasing_id())

    df_event.write.format("delta").mode("overwrite").save(f"{gold_path}/dim_event")

    return dim_event


if __name__=="__main__":

    spark = create_spark_session()

    df = build_dim_events(spark=spark,gold_path=gold_path)
    df.show()