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

def build_comment_text(spark, silver_path,gold_path):

    #read dim_video from gold

    #df_video = spark.read.format("delta").load(f"{gold_path}/dim_video")

    #read comments from sliver

    df_comments = spark.read.format("delta").load(f"{silver_path}/comments")

    #join to get video_sk

    #fact = df_comments.join(df_video.select("video_id","video_sk"), on = "video_id", how = "left")

    df_comments = df_comments.select("comment_id","video_id","textDisplay","authorDisplayName",
                                     "language_category","war_period","has_emoji")
    
    from pyspark.sql.functions import monotonically_increasing_id

    df_comments = df_comments.withColumn("comments_sk",monotonically_increasing_id())
    
    df_comments.write.format("delta").mode("overwrite").save(f"{gold_path}/dim_comments_text")

    return df_comments

if __name__=="__main__":

    spark = create_spark_session()

    df = build_comment_text(spark=spark,silver_path=silver_path,gold_path=gold_path)
    df.show()
