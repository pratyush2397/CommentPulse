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

def build_dim_date(spark, silver_path, gold_path):

    df_video = spark.read.format("delta").load(f"{silver_path}/videos")
    df_comments = spark.read.format("delta").load(f"{silver_path}/comments")

    #get dates from comments
    comment_dates = df_comments.select(F.col("published_date").alias("date"))

    #Get dates from videos
    video_dates = df_video.select(F.col("published_date").alias("date"))

    all_dates = comment_dates.union(video_dates).distinct()

    dim_date = all_dates.select(col("Date").alias("date"),year(col("Date")).\
                               alias("year"),month(col("Date")).\
                                alias("month"),quarter(col("Date")).\
                                    alias("quarter"),dayofmonth(col("date")).\
                                        alias("day_of_month"),dayofweek(col("date")).\
                                            alias("day_of_week"),weekofyear(col("date")).\
                                                alias("week_of_year"),date_format(col("Date"), "MMMM").\
                                                    alias("month_name"),date_format(col("Date"), "EEEE").\
                                                        alias("day_name"),when(dayofweek(col("Date")).isin(1, 7), True)\
                                                            .otherwise(False).alias("is_weekend"),date_format(col("Date"), "yyyy-MM").alias("year_month"))
    
    #Add surrogate Key
    from pyspark.sql.functions import monotonically_increasing_id

    dim_date = dim_date.withColumn("date_sk",monotonically_increasing_id()) 

    dim_date.write.format("delta").mode("overwrite").save(f"{gold_path}/dim_date")

    return dim_date

if __name__=="__main__":

    spark = create_spark_session()

    df = build_dim_date(spark=spark, silver_path=silver_path,gold_path=gold_path)
    df.show()




