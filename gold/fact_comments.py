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
gold_path = os.getenv("GOLD_PATH")

spark = create_spark_session()

def build_fact_comments(spark,silver_path,gold_path):

    dim_comment_text = spark.read.format("delta").load(f"{gold_path}/dim_comments_text")
    dim_video = spark.read.format("delta").load(f"{gold_path}/dim_video")
    dim_date = spark.read.format("delta").load(f"{gold_path}/dim_date")

    # Read silver comments (has all measures)
    comments = spark.read.format("delta").load(f"{silver_path}/comments")

    fact = comments.join(
    dim_comment_text.select("comment_id", "comments_sk"),
    on="comment_id", how="left")

    fact = fact.join(
    dim_video.select("video_id", "video_sk"),
    on="video_id", how="left")

    fact = fact.join(
    dim_date.select("date", "date_sk"),
    fact["published_date"] == dim_date["date"],
    how="left")

    from pyspark.sql.functions import monotonically_increasing_id

    fact = fact.withColumn("fact_id",monotonically_increasing_id())

    fact = fact.select("fact_id","comments_sk","video_sk","date_sk","war_period","likeCount","totalReplyCount","column_length")

    fact.write.format("delta").mode("overwrite").save(f"{gold_path}/fact_comments")

    return fact

if __name__=="__main__":

    fact =build_fact_comments(spark=spark,silver_path=silver_path,gold_path=gold_path)

    fact.show()


