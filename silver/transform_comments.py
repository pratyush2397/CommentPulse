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

import re
from pyspark.sql.types import StringType, BooleanType

def classify_language(text):
    if not text:
        return "other"
    if re.search(r'[\u0900-\u097F]', text):
        return "hindi"
    elif text.isascii():
        return "hinglish_or_english"
    else:
        return "other"
    
def has_emoji(text):
    if not text:
        return False
    emoji_pattern = re.compile(
        "[\U0001F300-\U0001F9FF"
        "\U0001F600-\U0001F64F"
        "\U00002702-\U000027B0]",
        flags=re.UNICODE
    )
    return bool(emoji_pattern.search(text))


# Register UDF at module level
language_udf = F.udf(classify_language, StringType())
has_emoji_udf = F.udf(has_emoji, BooleanType())

class Transform_comments:

    def __init__(self, spark,bronze_path,silver_path):

        self.spark = spark
        self.bronze_path = bronze_path
        self.silver_path = silver_path

    def read_bronze(self):

        df = self.spark.read.format("delta").load(f"{self.bronze_path}/comments")
        
        return df

    def clean(self,df):
        
        #drop nulls
        df = df.dropna(subset=["comment_id", "video_id", "textDisplay"])

        #remove_duplicates
        df = df.dropDuplicates(["comment_id"])


        #remove html tag
        df = df.withColumn("textDisplay", F.regexp_replace(F.col("textDisplay"), "<[^>]+>", ""))
        df = df.withColumn("textDisplay",F.regexp_replace(F.col("textDisplay"), "&amp;", "&"))
        df = df.withColumn("textDisplay",F.regexp_replace(F.col("textDisplay"), "&#39;", "'"))

        #remove too short comment

        df = df.filter(F.length(F.col("textDisplay"))>3)

        return df

    def enrich(self,df):
        
        df = df.withColumn("column_length", F.length(F.col("textDisplay")))

        df = df.withColumn("published_date", F.to_date(F.col("publishedAt")))
        df = df.withColumn("has_emoji", has_emoji_udf(F.col("textDisplay")))
        df = df.withColumn("war_period",F.when((F.col("published_date") >= "2026-01-01") & 
           (F.col("published_date") <= "2026-02-28"), "escalation")
     .when((F.col("published_date") >= "2026-03-01") & 
           (F.col("published_date") <= "2026-03-31"), "khomeini_death")
     .when((F.col("published_date") >= "2026-04-01") & 
           (F.col("published_date") <= "2026-04-30"), "peace_talks")
     .otherwise("pre_war"))
        
        df = df.withColumn("language_category", language_udf(F.col("textDisplay")))

        df.show()
        return df

    def write_silver(self,df):

        df.write.format("delta")\
            .mode("overwrite")\
                .partitionBy("video_id").save(f"{self.silver_path}/comments")

    def run(self):
        
        df = self.read_bronze()
        df = self.clean(df)
        df = self.enrich(df)
        self.write_silver(df)

if __name__== "__main__":

    spark = create_spark_session()
    comments = Transform_comments(
    spark=spark,
    bronze_path=bronze_path,
    silver_path=silver_path)

    comments.run()