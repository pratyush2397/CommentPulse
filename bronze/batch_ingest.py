import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, current_timestamp
from delta import configure_spark_with_delta_pip
from dotenv import load_dotenv
import pyspark.sql.functions as F
from datetime import datetime
import sys
import os

# Add project root to path
import os
os.environ["PYSPARK_PYTHON"] = r"C:\Users\harsh\AppData\Local\Programs\Python\Python310\python.exe"
os.environ["PYSPARK_DRIVER_PYTHON"] = r"C:\Users\harsh\AppData\Local\Programs\Python\Python310\python.exe"
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from ingestion.youtube_ingester import YoutubeIngester

logging.basicConfig(level=logging.INFO)
logger= logging.getLogger(__name__)

def create_spark_session() -> SparkSession:

    builder = (
        SparkSession.builder.appName("MarketPulse-bronze")
        .master("local[*]")
        .config("spark.sql.extensions","io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog","org.apache.spark.sql.delta.catalog.DeltaCatalog"))
    
    spark= configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    logger.info("Spark Session Created")

    return spark

def ingest_to_bronze(keyword,published_after,published_before,region_code = "IN", max_results = 50):
    load_dotenv("config/secrets_template.env")
    api_key = os.getenv("YOUTUBE_API_KEY")
    bronze_path = os.getenv("BRONZE_PATH")

    logger.info("Configuration loaded")

    ingester = YoutubeIngester(api_key,keyword,published_after,published_before,region_code,"video",max_results)

    videos_pandas_df,comments_pandas_df = ingester.ingest()

    logger.info("Step 2: Creating Spark Session")
    spark = create_spark_session()

    video_spark_df = spark.createDataFrame(videos_pandas_df)
    comments_spark_df = spark.createDataFrame(comments_pandas_df)

    captured_at = datetime.now().isoformat()

    video_spark_df = video_spark_df.withColumn("captured_at", lit(captured_at))
    comments_spark_df = comments_spark_df.withColumn("captured_at", lit(captured_at))
    video_spark_df = video_spark_df.withColumn("published_date", F.to_date("publishedAt"))

    video_spark_df.write.format("delta").mode("overwrite")\
        .partitionBy("published_date").save(f"{bronze_path}/videos")
    
    comments_spark_df.write.format("delta").mode("overwrite")\
        .partitionBy("video_id").save(f"{bronze_path}/comments")

if __name__ == "__main__":
    ingest_to_bronze(
        keyword="strait of hormuz",
        published_after="2026-01-01T00:00:00Z",
        published_before="2026-06-30T00:00:00Z",
        region_code="IN",
        max_results=10
    )   





